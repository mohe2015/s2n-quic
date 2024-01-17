// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

use super::{Chunk, Storage};

#[derive(Debug)]
pub struct FullCopy<'a, C: Storage + ?Sized>(&'a mut C);

impl<'a, C: Storage + ?Sized> FullCopy<'a, C> {
    #[inline]
    pub fn new(chunk: &'a mut C) -> Self {
        Self(chunk)
    }
}

impl<'a, C: Storage + ?Sized> Storage for FullCopy<'a, C> {
    type Error = C::Error;

    #[inline]
    fn buffered_len(&self) -> usize {
        self.0.buffered_len()
    }

    #[inline]
    fn buffer_is_empty(&self) -> bool {
        self.0.buffer_is_empty()
    }

    #[inline]
    fn read_chunk(&mut self, watermark: usize) -> Result<Chunk, Self::Error> {
        self.0.read_chunk(watermark)
    }

    #[inline]
    fn partial_copy_into<Dest: crate::buffer::writer::Storage>(
        &mut self,
        dest: &mut Dest,
    ) -> Result<Chunk, Self::Error> {
        // force the full copy
        self.0.copy_into(dest)?;
        Ok(Chunk::empty())
    }

    #[inline]
    fn copy_into<Dest: crate::buffer::writer::Storage>(
        &mut self,
        dest: &mut Dest,
    ) -> Result<(), Self::Error> {
        self.0.copy_into(dest)
    }
}
