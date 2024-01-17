// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

use crate::buffer::reader::storage::Chunk;
use bytes::{buf::UninitSlice, Bytes, BytesMut};

mod buf;
mod byte_queue;
mod discard;
mod empty;
mod limit;
mod tracked;
mod uninit_slice;

pub use buf::BufMut;
pub use discard::Discard;
pub use empty::Empty;
pub use limit::Limit;
pub use tracked::Tracked;

pub trait Storage {
    const SPECIALIZES_BYTES: bool = false;
    const SPECIALIZES_BYTES_MUT: bool = false;

    fn put_slice(&mut self, bytes: &[u8]);

    #[inline(always)]
    fn put_uninit_slice<F, Error>(&mut self, payload_len: usize, f: F) -> Result<bool, Error>
    where
        F: FnOnce(&mut UninitSlice) -> Result<(), Error>,
    {
        // we can specialize on an empty payload
        ensure!(payload_len == 0, Ok(false));

        f(UninitSlice::new(&mut []))?;

        Ok(true)
    }

    fn remaining_capacity(&self) -> usize;

    #[inline]
    fn has_remaining_capacity(&self) -> bool {
        self.remaining_capacity() > 0
    }

    #[inline]
    fn put_bytes(&mut self, bytes: Bytes) {
        self.put_slice(&bytes);
    }

    #[inline]
    fn put_bytes_mut(&mut self, bytes: BytesMut) {
        self.put_slice(&bytes);
    }

    #[inline]
    fn put_chunk(&mut self, chunk: Chunk) {
        match chunk {
            Chunk::Slice(v) => self.put_slice(v),
            Chunk::Bytes(v) => self.put_bytes(v),
            Chunk::BytesMut(v) => self.put_bytes_mut(v),
        }
    }

    #[inline]
    fn limit(&mut self, max_len: usize) -> Limit<Self> {
        Limit::new(self, max_len)
    }

    #[inline]
    fn tracked(&mut self) -> Tracked<Self> {
        Tracked::new(self)
    }
}
