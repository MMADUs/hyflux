use bytes::{BufMut, BytesMut};
use http::header::AsHeaderName;
use http::response::{Parts, Response};
use http::{HeaderMap, HeaderName, HeaderValue, StatusCode, Version};

use super::case::{CaseHeaderName, IntoCaseHeaderName};

const MAX_HEADER_COUNT: usize = 4096;
const INIT_HEADER_SIZE: usize = 8;
const BUILD_HEADER_BUFFER: usize = 512;

const CRLF: &[u8; 2] = b"\r\n";
const HEADER_DELIMITER: &[u8; 2] = b": ";
const EMPTY_SPACE: &[u8; 1] = b" ";

pub type CaseMap = HeaderMap<CaseHeaderName>;

/// a type for response headers
#[derive(Debug, Clone)]
pub struct ResponseHeader {
    pub metadata: Parts,
    pub header_case: CaseMap,
    pub reason_phrase: Option<String>,
}

impl ResponseHeader {
    /// build a new response header
    pub fn build<S>(status_code: S, version: Version, size: Option<usize>) -> Self
    where
        S: TryInto<StatusCode>,
    {
        let capacity = Self::serve_capacity(size);

        let status_code = status_code
            .try_into()
            .map_err(|_| format!("Invalid status code"))
            .expect("Failed to convert status code");

        let (mut parts, _) = Response::builder()
            .status(status_code)
            .version(version)
            .body(())
            .expect("Failed to create response metadata")
            .into_parts();

        parts.headers.reserve(capacity);

        ResponseHeader {
            metadata: parts,
            header_case: CaseMap::with_capacity(capacity),
            reason_phrase: None,
        }
    }

    /// helper function used to served the capacity size for response headers
    fn serve_capacity(size: Option<usize>) -> usize {
        std::cmp::min(size.unwrap_or(INIT_HEADER_SIZE), MAX_HEADER_COUNT)
    }

    /// get response status code
    ///
    /// use .as_u16() to get the status as u16
    /// use .as_str() to get status code as str
    pub fn get_status_code(&self) -> &StatusCode {
        &self.metadata.status
    }

    /// get response version
    pub fn get_version(&self) -> &Version {
        &self.metadata.version
    }

    /// get raw response version
    pub fn get_raw_version(&self) -> &str {
        match self.metadata.version {
            Version::HTTP_09 => "HTTP/0.9",
            Version::HTTP_10 => "HTTP/1.0",
            Version::HTTP_11 => "HTTP/1.1",
            Version::HTTP_2 => "HTTP/2",
            _ => panic!("unsupported version"),
        }
    }

    /// append new response header
    /// this would add a new header without replacing existing header with the same name
    pub fn append_header<N, V>(&mut self, name: N, value: V)
    where
        N: IntoCaseHeaderName,
        V: TryInto<HeaderValue>,
    {
        let case_header_name = name.into_case_header_name();

        let header_name: HeaderName = case_header_name
            .as_slice()
            .try_into()
            .map_err(|_| format!("Invalid header name"))
            .expect("Failed to convert header name");

        let header_value = value
            .try_into()
            .map_err(|_| format!("Invalid header value"))
            .expect("Failed to convert header value");

        self.header_case
            .append(header_name.clone(), case_header_name);
        self.metadata.headers.append(header_name, header_value);
    }

    /// insert new response header
    /// this would add a new header and replace exisiting header with the same name
    pub fn insert_header<N, V>(&mut self, name: N, value: V)
    where
        N: IntoCaseHeaderName,
        V: TryInto<HeaderValue>,
    {
        let case_header_name = name.into_case_header_name();

        let header_name: HeaderName = case_header_name
            .as_slice()
            .try_into()
            .map_err(|_| format!("Invalid header name"))
            .expect("Failed to convert header name");

        let header_value = value
            .try_into()
            .map_err(|_| format!("Invalid header value"))
            .expect("Failed to convert header value");

        self.header_case
            .insert(header_name.clone(), case_header_name);
        self.metadata.headers.insert(header_name, header_value);
    }

    /// remove response header
    pub fn remove_header<'a, N: ?Sized>(&mut self, name: &'a N)
    where
        &'a N: AsHeaderName,
    {
        self.header_case.remove(name);
        self.metadata.headers.remove(name);
    }

    /// get response headers value
    /// this would retrive all the headers value with the same name
    pub fn get_headers<N>(&self, name: N) -> Vec<&HeaderValue>
    where
        N: AsHeaderName,
    {
        self.metadata.headers.get_all(name).iter().collect()
    }

    /// get response header value
    /// this would only retrieve one of the header with the same name
    pub fn get_header<N>(&self, name: N) -> Option<&HeaderValue>
    where
        N: AsHeaderName,
    {
        self.metadata.headers.get(name)
    }

    /// set response version
    pub fn set_version(&mut self, version: Version) {
        self.metadata.version = version
    }

    /// set response status code
    pub fn set_status_code<S>(&mut self, status: S)
    where
        S: TryInto<StatusCode>,
    {
        self.metadata.status = status
            .try_into()
            .map_err(|_| format!("Invalid status code"))
            .expect("Failed to convert status code")
    }

    /// set reason phrase for the given status
    pub fn set_reason_phrase(&mut self, reason_phrase: Option<&str>) {
        if reason_phrase == self.metadata.status.canonical_reason() {
            self.reason_phrase = None;
            return;
        }
        self.reason_phrase = reason_phrase.map(str::to_string);
    }

    /// get response reason phrase
    /// when the reason phrase is not set, otherwise use the default reason
    pub fn get_reason_phrase(&self) -> Option<&str> {
        self.reason_phrase
            .as_deref()
            .or_else(|| self.metadata.status.canonical_reason())
    }

    /// build response header to buffer
    /// used wire to session buffer
    pub fn build_to_buffer(&self) -> BytesMut {
        let mut buffer = BytesMut::with_capacity(BUILD_HEADER_BUFFER);

        let version = self.get_raw_version().as_bytes();
        buffer.put_slice(version);
        buffer.put_slice(EMPTY_SPACE);

        let status = self.get_status_code().as_str().as_bytes();
        buffer.put_slice(status);
        buffer.put_slice(EMPTY_SPACE);

        if let Some(reason) = self.get_reason_phrase() {
            buffer.put_slice(reason.as_bytes());
        }
        buffer.put_slice(CRLF);

        let iter = self.header_case.iter().zip(self.metadata.headers.iter());
        for ((header, case_header), (header2, val)) in iter {
            if header != header2 {
                // in case the header iteration order changes in future versions of HMap
                panic!("header iter mismatch {}, {}", header, header2)
            }
            buffer.put_slice(case_header.as_slice());
            buffer.put_slice(HEADER_DELIMITER);
            buffer.put_slice(val.as_ref());
            buffer.put_slice(CRLF);
        }
        buffer.put_slice(CRLF);

        buffer
    }
}
