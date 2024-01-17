// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

use super::{Chunk, Storage};
use bytes::{buf::UninitSlice, Bytes, BytesMut};

pub struct Limit<'a, C: Storage + ?Sized> {
    chunk: &'a mut C,
    remaining_capacity: usize,
}

impl<'a, C: Storage + ?Sized> Limit<'a, C> {
    #[inline]
    pub fn new(chunk: &'a mut C, remaining_capacity: usize) -> Self {
        let remaining_capacity = chunk.remaining_capacity().min(remaining_capacity);
        Self {
            chunk,
            remaining_capacity,
        }
    }
}

impl<'a, C: Storage + ?Sized> Storage for Limit<'a, C> {
    const SPECIALIZES_BYTES: bool = C::SPECIALIZES_BYTES;
    const SPECIALIZES_BYTES_MUT: bool = C::SPECIALIZES_BYTES_MUT;

    #[inline]
    fn put_slice(&mut self, bytes: &[u8]) {
        debug_assert!(bytes.len() <= self.remaining_capacity);
        self.chunk.put_slice(bytes);
        self.remaining_capacity -= bytes.len();
    }

    #[inline(always)]
    fn put_uninit_slice<F, Error>(&mut self, payload_len: usize, f: F) -> Result<bool, Error>
    where
        F: FnOnce(&mut UninitSlice) -> Result<(), Error>,
    {
        debug_assert!(payload_len <= self.remaining_capacity);
        let did_write = self.chunk.put_uninit_slice(payload_len, f)?;
        if did_write {
            self.remaining_capacity -= payload_len;
        }
        Ok(did_write)
    }

    #[inline]
    fn remaining_capacity(&self) -> usize {
        self.chunk.remaining_capacity().min(self.remaining_capacity)
    }

    #[inline]
    fn has_remaining_capacity(&self) -> bool {
        self.remaining_capacity > 0 && self.chunk.has_remaining_capacity()
    }

    #[inline]
    fn put_bytes(&mut self, bytes: Bytes) {
        let len = bytes.len();
        debug_assert!(len <= self.remaining_capacity);
        self.chunk.put_bytes(bytes);
        self.remaining_capacity -= len;
    }

    #[inline]
    fn put_bytes_mut(&mut self, bytes: BytesMut) {
        let len = bytes.len();
        debug_assert!(len <= self.remaining_capacity);
        self.chunk.put_bytes_mut(bytes);
        self.remaining_capacity -= len;
    }

    #[inline]
    fn put_chunk(&mut self, chunk: Chunk) {
        let len = chunk.len();
        debug_assert!(len <= self.remaining_capacity);
        self.chunk.put_chunk(chunk);
        self.remaining_capacity -= len;
    }
}
