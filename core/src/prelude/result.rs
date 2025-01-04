use std::error::Error as ErrorTrait;
//use std::fmt;
//use std::fmt::Debug;
use std::result::Result as StdResult;

// custom error
pub struct Error {
    pub cause: Option<Box<(dyn ErrorTrait + Send + Sync)>>,
}

// boxed custom error
pub type BoxedError = Box<Error>;

// result with boxed custom error
pub type Result<T, E = BoxedError> = StdResult<T, E>;
