// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

use super::{storage, Reader, Storage, VarInt};
use crate::buffer;

/// Wraps a single slice as a reader.
///
/// This can be used for scenarios where the entire stream is buffered and known up-front.
#[derive(Debug)]
pub struct Slice<'a, C> {
    chunk: &'a mut C,
    current_offset: VarInt,
    final_offset: VarInt,
}

impl<'a, C> Slice<'a, C>
where
    C: Storage,
{
    #[inline]
    pub fn new(chunk: &'a mut C) -> Result<Self, buffer::Error> {
        let final_offset = VarInt::try_from(chunk.buffered_len())
            .ok()
            .ok_or(buffer::Error::OutOfRange)?;
        Ok(Self {
            chunk,
            current_offset: VarInt::ZERO,
            final_offset,
        })
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.chunk.buffer_is_empty()
    }
}

impl<'a, C> Storage for Slice<'a, C>
where
    C: Storage,
{
    type Error = C::Error;

    #[inline]
    fn buffered_len(&self) -> usize {
        self.chunk.buffered_len()
    }

    #[inline]
    fn read_chunk(&mut self, watermark: usize) -> Result<storage::Chunk, Self::Error> {
        let chunk = self.chunk.read_chunk(watermark)?;
        self.current_offset += chunk.len();
        Ok(chunk)
    }

    #[inline]
    fn partial_copy_into<Dest: crate::buffer::writer::Storage>(
        &mut self,
        dest: &mut Dest,
    ) -> Result<storage::Chunk, Self::Error> {
        let len = self.buffered_len().min(dest.remaining_capacity());
        let chunk = self.chunk.partial_copy_into(dest)?;
        self.current_offset += len;
        Ok(chunk)
    }

    #[inline]
    fn copy_into<Dest: crate::buffer::writer::Storage>(
        &mut self,
        dest: &mut Dest,
    ) -> Result<(), Self::Error> {
        let len = self.buffered_len().min(dest.remaining_capacity());
        self.chunk.copy_into(dest)?;
        self.current_offset += len;
        Ok(())
    }
}

impl<'a, C> Reader for Slice<'a, C>
where
    C: Storage,
{
    #[inline]
    fn current_offset(&self) -> VarInt {
        self.current_offset
    }

    #[inline]
    fn final_offset(&self) -> Option<VarInt> {
        Some(self.final_offset)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slice_test() {
        let mut storage: &[u8] = &[1, 2, 3, 4];
        let mut reader = Slice::new(&mut storage).unwrap();

        assert_eq!(reader.current_offset(), VarInt::ZERO);
        assert_eq!(reader.final_offset(), Some(VarInt::from_u8(4)));

        let mut dest: &mut [u8] = &mut [0; 4];
        let chunk = reader.partial_copy_into(&mut dest).unwrap();
        assert_eq!(&*chunk, &[1, 2, 3, 4]);

        assert_eq!(reader.current_offset(), VarInt::from_u8(4));
        assert!(reader.buffer_is_empty());
    }
}
