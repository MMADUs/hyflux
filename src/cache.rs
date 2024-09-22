/**
 * Copyright (c) 2024-2025 ArcX, Inc.
 *
 * This file is part of ArcX Gateway
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

use std::any::Any;
use std::sync::Arc;
use std::fmt::Debug;

use pingora::cache::storage::{HandleHit, HandleMiss, HitHandler, MissHandler};
use pingora::cache::key::{CacheHashKey, CacheKey, CompactCacheKey, HashBinary};
use pingora::cache::max_file_size::ERR_RESPONSE_TOO_LARGE;
use pingora::cache::trace::SpanHandle;
use pingora::cache::{CacheMeta, PurgeType, Storage};
use pingora::{Error, Result};

use scc::HashMap;
use serde::{Deserialize, Serialize};
use ahash::RandomState;
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use http::header;

/// Content types that skip cache compression by default
pub const SKIP_COMPRESSION: &'static [&'static str] = &[
    "image/avif",
    "image/webp",
    "image/png",
    "image/jpeg",
    "font/woff2",
    "font/woff",
    "video/webm",
    "video/ogg",
    "video/mpeg",
    "video/mp4",
    "application/zip",
    "application/gzip",
];

pub trait CacheCompression: Default + Debug + Clone + Send + Sync + 'static {
    fn encode(&self, buf: Bytes) -> Result<Bytes>;
    fn decode(&self, buf: &Bytes) -> Result<Bytes>;
    fn decode_range(&mut self, buf: &Bytes, range_start: usize, range_end: usize) -> Result<Bytes> {
        self.decode(buf)
            .map(|buf| buf.slice(range_start..range_end))
    }

    fn should_compress(&self, content_type: Option<&str>) -> bool {
        if let Some(content_type) = content_type {
            if SKIP_COMPRESSION.contains(&content_type) {
                return false;
            }
        }
        true
    }

    fn compressed(&self) -> bool {
        true
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct NoCompression;

impl CacheCompression for NoCompression {
    fn encode(&self, buf: Bytes) -> Result<Bytes> {
        Ok(buf)
    }

    fn decode(&self, buf: &Bytes) -> Result<Bytes> {
        Ok(buf.clone())
    }

    fn should_compress(&self, _content_type: Option<&str>) -> bool {
        false
    }

    fn compressed(&self) -> bool {
        false
    }
}

type BinaryMeta = (Bytes, Bytes);

pub type SharedHashMap = Arc<HashMap<HashBinary, SccCacheObject, RandomState>>;

/// Cache that uses scc::HashMap.
/// Does not support streaming partial writes
#[derive(Clone, Debug)]
pub struct SccMemoryCache<C: CacheCompression = NoCompression> {
    pub cache: SharedHashMap,
    /// Maximum allowed body size for caching
    pub max_file_size_bytes: Option<usize>,
    /// Will reject cache admissions with empty body responses
    pub reject_empty_body: bool,
    /// Compression that will be used
    pub compression: C,
}

impl SccMemoryCache {
    pub fn from_map(cache: SharedHashMap) -> Self {
        SccMemoryCache {
            cache,
            max_file_size_bytes: None,
            reject_empty_body: false,
            compression: NoCompression,
        }
    }

    pub fn new() -> Self {
        SccMemoryCache {
            cache: Arc::new(HashMap::with_hasher(RandomState::new())),
            max_file_size_bytes: None,
            reject_empty_body: false,
            compression: NoCompression,
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        SccMemoryCache {
            cache: Arc::new(HashMap::with_capacity_and_hasher(
                capacity,
                RandomState::new(),
            )),
            max_file_size_bytes: None,
            reject_empty_body: false,
            compression: NoCompression,
        }
    }
}

impl<C: CacheCompression> SccMemoryCache<C> {
    pub fn with_compression<T: CacheCompression>(self, compression: T) -> SccMemoryCache<T> {
        SccMemoryCache {
            compression,
            cache: self.cache,
            max_file_size_bytes: self.max_file_size_bytes,
            reject_empty_body: self.reject_empty_body,
        }
    }

    pub fn with_max_file_size(mut self, max_bytes: Option<usize>) -> Self {
        self.max_file_size_bytes = max_bytes;
        self
    }

    pub fn with_reject_empty_body(mut self, should_error: bool) -> Self {
        self.reject_empty_body = should_error;
        self
    }
}

#[async_trait]
impl<C: CacheCompression> Storage for SccMemoryCache<C> {
    async fn lookup(
        &'static self,
        key: &CacheKey,
        _trace: &SpanHandle,
    ) -> Result<Option<(CacheMeta, HitHandler)>> {
        let hash = key.combined_bin();

        let cache_object;
        if let Some(obj) = self.cache.get_async(&hash).await {
            cache_object = obj.get().clone();
        } else {
            return Ok(None);
        }
        let meta = CacheMeta::deserialize(&cache_object.meta.0, &cache_object.meta.1)?;
        Ok(Some((
            meta,
            Box::new(SccHitHandler::new(cache_object, self.clone())),
        )))
    }

    async fn get_miss_handler(
        &'static self,
        key: &CacheKey,
        meta: &CacheMeta,
        _trace: &SpanHandle,
    ) -> Result<MissHandler> {
        let hash = key.combined_bin();
        let raw_meta = meta.serialize()?;

        let already_compressed = meta
            .headers()
            .get(header::CONTENT_ENCODING)
            .map(|v| !v.is_empty())
            .unwrap_or(false);
        let content_type = meta
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok());
        let compress_content_type = self.compression.should_compress(content_type);

        let meta = (Bytes::from(raw_meta.0), Bytes::from(raw_meta.1));
        let miss_handler = SccMissHandler {
            body_buf: BytesMut::new(),
            meta,
            key: hash,
            inner: self.clone(),
            compress: self.compression.compressed() && !already_compressed && compress_content_type,
        };
        Ok(Box::new(miss_handler))
    }

    async fn purge(
        &'static self,
        key: &CompactCacheKey,
        _purge_type: PurgeType,
        _trace: &SpanHandle,
    ) -> Result<bool> {
        let hash = key.combined_bin();
        Ok(self.cache.remove(&hash).is_some())
    }

    async fn update_meta(
        &'static self,
        key: &CacheKey,
        meta: &CacheMeta,
        _trace: &SpanHandle,
    ) -> Result<bool> {
        let hash = key.combined_bin();
        let new_meta = meta.serialize()?;
        let new_meta = (Bytes::from(new_meta.0), Bytes::from(new_meta.1));

        let updated = self
            .cache
            .update_async(&hash, move |_, value| {
                value.meta = new_meta;
            })
            .await;
        if let Some(()) = updated {
            Ok(true)
        } else {
            Err(Error::create(
                pingora::ErrorType::Custom("No meta found for update_meta"),
                pingora::ErrorSource::Internal,
                Some(format!("key = {:?}", key).into()),
                None,
            ))
        }
    }

    fn support_streaming_partial_write(&self) -> bool {
        false
    }

    fn as_any(&self) -> &(dyn Any + Send + Sync) {
        self
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash, Debug)]
pub struct SccCacheObject {
    meta: BinaryMeta,
    body: Bytes,
    /// Uncompressed len of body
    len: usize,
    /// Is body compressed
    compressed: bool,
}

pub struct SccHitHandler<C: CacheCompression> {
    cache_object: SccCacheObject,
    inner: SccMemoryCache<C>,
    done: bool,
    range_start: usize,
    range_end: usize,
}

impl<C: CacheCompression> SccHitHandler<C> {
    pub(crate) fn new(cache_object: SccCacheObject, inner: SccMemoryCache<C>) -> Self {
        let len = cache_object.len;
        SccHitHandler {
            cache_object,
            inner,
            done: false,
            range_start: 0,
            range_end: len,
        }
    }
}

#[async_trait]
impl<C: CacheCompression> HandleHit for SccHitHandler<C> {
    async fn read_body(&mut self) -> Result<Option<Bytes>> {
        if self.done {
            Ok(None)
        } else {
            self.done = true;
            if self.cache_object.compressed {
                self.inner
                    .compression
                    .decode_range(&self.cache_object.body, self.range_start, self.range_end)
                    .map(Some)
            } else {
                Ok(Some(
                    self.cache_object
                        .body
                        .slice(self.range_start..self.range_end),
                ))
            }
        }
    }

    async fn finish(
        mut self: Box<Self>,
        _storage: &'static (dyn Storage + Sync),
        _key: &CacheKey,
        _trace: &SpanHandle,
    ) -> Result<()> {
        Ok(())
    }

    fn can_seek(&self) -> bool {
        true
    }

    fn seek(&mut self, start: usize, end: Option<usize>) -> Result<()> {
        if start >= self.cache_object.len {
            return Error::e_explain(
                pingora::ErrorType::InternalError,
                format!(
                    "seek start out of range {start} >= {}",
                    self.cache_object.len
                ),
            );
        }
        self.range_start = start;
        if let Some(end) = end {
            self.range_end = std::cmp::min(self.cache_object.len, end);
        }
        self.done = false;
        Ok(())
    }

    fn as_any(&self) -> &(dyn Any + Send + Sync) {
        self
    }
}

#[derive(Debug)]
struct SccMissHandler<C: CacheCompression> {
    meta: BinaryMeta,
    key: HashBinary,
    body_buf: BytesMut,
    inner: SccMemoryCache<C>,
    compress: bool,
}

#[async_trait]
impl<C: CacheCompression> HandleMiss for SccMissHandler<C> {
    async fn write_body(&mut self, data: bytes::Bytes, _eof: bool) -> Result<()> {
        if let Some(max_file_size_bytes) = self.inner.max_file_size_bytes {
            if self.body_buf.len() + data.len() > max_file_size_bytes {
                return Error::e_explain(
                    ERR_RESPONSE_TOO_LARGE,
                    format!(
                        "writing data of size {} bytes would exceed max file size of {} bytes",
                        data.len(),
                        max_file_size_bytes
                    ),
                );
            }
        }
        self.body_buf.extend_from_slice(&data);
        Ok(())
    }

    async fn finish(self: Box<Self>) -> Result<usize> {
        let uncompressed_len = self.body_buf.len();
        if uncompressed_len == 0 && self.inner.reject_empty_body {
            let err = Error::create(
                pingora::ErrorType::Custom("cache write error: empty body"),
                pingora::ErrorSource::Internal,
                None,
                None,
            );
            return Err(err);
        }
        let body = if self.compress {
            self.inner.compression.encode(self.body_buf.freeze())?
        } else {
            self.body_buf.freeze()
        };
        let size = body.len() + self.meta.0.len() + self.meta.1.len();
        let cache_object = SccCacheObject {
            body,
            meta: self.meta,
            len: uncompressed_len,
            compressed: self.compress,
        };
        self.inner
            .cache
            .insert_async(self.key, cache_object)
            .await
            .ok();
        Ok(size)
    }
}