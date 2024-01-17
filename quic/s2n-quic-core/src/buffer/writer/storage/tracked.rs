// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

use super::{Chunk, Storage};
use bytes::{buf::UninitSlice, Bytes, BytesMut};

pub struct Tracked<'a, C: Storage + ?Sized> {
    chunk: &'a mut C,
    written: usize,
}

impl<'a, C: Storage + ?Sized> Tracked<'a, C> {
    #[inline]
    pub fn new(chunk: &'a mut C) -> Self {
        Self { chunk, written: 0 }
    }

    #[inline]
    pub fn written_len(&self) -> usize {
        self.written
    }
}

impl<'a, C: Storage + ?Sized> Storage for Tracked<'a, C> {
    const SPECIALIZES_BYTES: bool = C::SPECIALIZES_BYTES;
    const SPECIALIZES_BYTES_MUT: bool = C::SPECIALIZES_BYTES_MUT;

    #[inline]
    fn put_slice(&mut self, bytes: &[u8]) {
        self.chunk.put_slice(bytes);
        self.written += bytes.len();
    }

    #[inline(always)]
    fn put_uninit_slice<F, Error>(&mut self, payload_len: usize, f: F) -> Result<bool, Error>
    where
        F: FnOnce(&mut UninitSlice) -> Result<(), Error>,
    {
        let did_write = self.chunk.put_uninit_slice(payload_len, f)?;
        if did_write {
            self.written += payload_len;
        }
        Ok(did_write)
    }

    #[inline]
    fn remaining_capacity(&self) -> usize {
        self.chunk.remaining_capacity()
    }

    #[inline]
    fn has_remaining_capacity(&self) -> bool {
        self.chunk.has_remaining_capacity()
    }

    #[inline]
    fn put_bytes(&mut self, bytes: Bytes) {
        let len = bytes.len();
        self.chunk.put_bytes(bytes);
        self.written += len;
    }

    #[inline]
    fn put_bytes_mut(&mut self, bytes: BytesMut) {
        let len = bytes.len();
        self.chunk.put_bytes_mut(bytes);
        self.written += len;
    }

    #[inline]
    fn put_chunk(&mut self, chunk: Chunk) {
        let len = chunk.len();
        self.chunk.put_chunk(chunk);
        self.written += len;
    }
}
