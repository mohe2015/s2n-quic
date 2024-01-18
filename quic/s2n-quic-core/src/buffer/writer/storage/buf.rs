// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

use super::{Storage, UninitSlice};

pub struct BufMut<'a, T: bytes::BufMut> {
    buf_mut: &'a mut T,
}

impl<'a, T: bytes::BufMut> BufMut<'a, T> {
    #[inline]
    pub fn new(buf_mut: &'a mut T) -> Self {
        Self { buf_mut }
    }
}

impl<'a, T: bytes::BufMut> Storage for BufMut<'a, T> {
    #[inline]
    fn put_slice(&mut self, bytes: &[u8]) {
        self.buf_mut.put_slice(bytes);
    }

    #[inline]
    fn remaining_capacity(&self) -> usize {
        self.buf_mut.remaining_mut()
    }

    #[inline]
    fn put_uninit_slice<F, Error>(&mut self, payload_len: usize, f: F) -> Result<bool, Error>
    where
        F: FnOnce(&mut UninitSlice) -> Result<(), Error>,
    {
        let chunk = self.buf_mut.chunk_mut();
        ensure!(chunk.len() >= payload_len, Ok(false));

        f(&mut chunk[..payload_len])?;

        unsafe {
            self.buf_mut.advance_mut(payload_len);
        }

        Ok(true)
    }
}

macro_rules! impl_buf_mut {
    ($ty:ty $(, $reserve:ident)?) => {
        impl Storage for $ty {
            #[inline]
            fn put_slice(&mut self, bytes: &[u8]) {
                bytes::BufMut::put_slice(self, bytes);
            }

            #[inline]
            fn remaining_capacity(&self) -> usize {
                bytes::BufMut::remaining_mut(self)
            }

            #[inline]
            fn put_uninit_slice<F, Error>(
                &mut self,
                payload_len: usize,
                f: F,
            ) -> Result<bool, Error>
            where
                F: FnOnce(&mut UninitSlice) -> Result<(), Error>,
            {
                use bytes::BufMut;

                $(
                    self.$reserve(payload_len);
                )?

                let chunk = self.chunk_mut();
                ensure!(chunk.len() >= payload_len, Ok(false));

                f(&mut chunk[..payload_len])?;

                unsafe {
                    self.advance_mut(payload_len);
                }

                Ok(true)
            }
        }
    };
}

impl_buf_mut!(bytes::BytesMut, reserve);
impl_buf_mut!(alloc::vec::Vec<u8>, reserve);
impl_buf_mut!(&mut [u8]);
impl_buf_mut!(&mut [core::mem::MaybeUninit<u8>]);

#[cfg(test)]
mod tests {
    use crate::buffer::{reader::Storage as _, writer::Storage as _};

    #[test]
    fn vec_test() {
        let mut buffer: Vec<u8> = vec![];
        assert_eq!(buffer.remaining_capacity(), isize::MAX as usize);

        let expected = &b"hello world!"[..];

        let mut source = expected;

        source.copy_into(&mut buffer).unwrap();

        assert_eq!(&buffer, expected);
    }
}
