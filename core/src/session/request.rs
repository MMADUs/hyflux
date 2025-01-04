use bytes::{BufMut, BytesMut};
use http::header::AsHeaderName;
use http::request::{Parts, Request};
use http::{HeaderMap, HeaderName, HeaderValue, Method, Uri, Version};

use super::case::{CaseHeaderName, IntoCaseHeaderName};

const MAX_HEADER_COUNT: usize = 4096;
const INIT_HEADER_SIZE: usize = 8;
const BUILD_HEADER_BUFFER: usize = 512;

const CRLF: &[u8; 2] = b"\r\n";
const HEADER_DELIMITER: &[u8; 2] = b": ";
const EMPTY_SPACE: &[u8; 1] = b" ";

pub type CaseMap = HeaderMap<CaseHeaderName>;

/// a type for request headers
#[derive(Debug, Clone)]
pub struct RequestHeader {
    pub metadata: Parts,
    pub header_case: CaseMap,
    pub raw_path: Vec<u8>,
}

impl RequestHeader {
    /// build a new request headers
    pub fn build<M>(method: M, raw_path: &str, version: Version, size: Option<usize>) -> Self
    where
        M: TryInto<Method>,
    {
        let capacity = Self::serve_capacity(size);

        let method = method
            .try_into()
            .map_err(|_| format!("Invalid method"))
            .expect("Failed to convert method");

        let (mut parts, _) = Request::builder()
            .method(method)
            .uri(raw_path.as_bytes())
            .version(version)
            .body(())
            .expect("Failed to create base request")
            .into_parts();

        parts.headers.reserve(capacity);

        RequestHeader {
            metadata: parts,
            header_case: CaseMap::with_capacity(capacity),
            raw_path: raw_path.as_bytes().to_vec(),
        }
    }

    /// helper function used to served the capacity size for request headers
    fn serve_capacity(size: Option<usize>) -> usize {
        std::cmp::min(size.unwrap_or(INIT_HEADER_SIZE), MAX_HEADER_COUNT)
    }

    /// get request method
    pub fn get_method(&self) -> &Method {
        &self.metadata.method
    }

    /// get raw request path
    pub fn get_raw_path(&self) -> &[u8] {
        &self.raw_path
    }

    /// get request uri
    /// 
    /// use .path() to get uri path as &str
    /// use .host() to get uri host as &str
    /// use .query() to get uri query param as &str
    pub fn get_uri(&self) -> &Uri {
        &self.metadata.uri
    }

    /// get request version
    pub fn get_version(&self) -> &Version {
        &self.metadata.version
    }

    /// get raw request version
    pub fn get_raw_version(&self) -> &str {
        match self.metadata.version {
            Version::HTTP_09 => "HTTP/0.9",
            Version::HTTP_10 => "HTTP/1.0",
            Version::HTTP_11 => "HTTP/1.1",
            Version::HTTP_2 => "HTTP/2",
            _ => panic!("unsupported version"),
        }
    }

    /// append new request header
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

    /// insert new request header
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

    /// set request method
    pub fn set_method(&mut self, method: Method) {
        self.metadata.method = method
    }

    /// set request uri
    pub fn set_uri(&mut self, uri: Uri) {
        self.metadata.uri = uri
    }

    /// set request version
    pub fn set_version(&mut self, version: Version) {
        self.metadata.version = version
    }

    /// build request header to buffer
    /// used wire to session buffer
    pub fn build_to_buffer(&self) -> BytesMut {
        let mut buffer = BytesMut::with_capacity(BUILD_HEADER_BUFFER);

        let method = self.get_method().as_str().as_bytes();
        buffer.put_slice(method);
        buffer.put_slice(EMPTY_SPACE);

        let path = self.get_raw_path();
        buffer.put_slice(path);
        buffer.put_slice(EMPTY_SPACE);

        let version = self.get_raw_version().as_bytes();
        buffer.put_slice(version);
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
