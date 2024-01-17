// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum Error<Reader = core::convert::Infallible> {
    /// An invalid data range was provided
    OutOfRange,
    /// The provided final size was invalid for the buffer's state
    InvalidFin,
    /// The provided reader failed
    ReaderError(Reader),
}

impl<Reader> From<Reader> for Error<Reader> {
    #[inline]
    fn from(reader: Reader) -> Self {
        Self::ReaderError(reader)
    }
}

impl Error {
    #[inline]
    pub fn mapped<Reader>(error: Error) -> Error<Reader> {
        match error {
            Error::OutOfRange => Error::OutOfRange,
            Error::InvalidFin => Error::InvalidFin,
            Error::ReaderError(_) => unreachable!(),
        }
    }
}

#[cfg(feature = "std")]
impl<Reader: std::error::Error> std::error::Error for Error<Reader> {}

impl<Reader: core::fmt::Display> core::fmt::Display for Error<Reader> {
    fn fmt(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
        match self {
            Self::OutOfRange => write!(f, "write extends out of the maximum possible offset"),
            Self::InvalidFin => write!(
                f,
                "write modifies the final offset in a non-compliant manner"
            ),
            Self::ReaderError(reader) => write!(f, "the provided reader failed with: {reader}"),
        }
    }
}
