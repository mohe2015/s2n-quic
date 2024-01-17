// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

use super::{storage, Reader, Storage, VarInt};
use crate::{buffer, ensure};

/// Implements an incremental reader that handles temporary chunks as the stream data
///
/// This is useful for scenarios where the the stream isn't completely buffered in memory and
/// chunks come in gradually.
#[derive(Debug, Default)]
pub struct Incremental {
    current_offset: VarInt,
    final_offset: Option<VarInt>,
}

impl Incremental {
    #[inline]
    pub fn with_chunk<'a, C: Storage>(
        &'a mut self,
        chunk: &'a mut C,
        is_fin: bool,
    ) -> Result<WithChunk<'a, C>, buffer::Error> {
        let mut chunk = WithChunk {
            incremental: self,
            chunk,
        };

        if is_fin {
            chunk.set_fin()?;
        } else {
            ensure!(
                chunk.incremental.final_offset.is_none(),
                Err(buffer::Error::InvalidFin)
            );
        }

        Ok(chunk)
    }
}

impl Storage for Incremental {
    type Error = core::convert::Infallible;

    #[inline]
    fn buffered_len(&self) -> usize {
        0
    }

    #[inline]
    fn read_chunk(&mut self, _watermark: usize) -> Result<storage::Chunk, Self::Error> {
        Ok(Default::default())
    }

    #[inline]
    fn partial_copy_into<Dest: crate::buffer::writer::Storage>(
        &mut self,
        _dest: &mut Dest,
    ) -> Result<storage::Chunk, Self::Error> {
        Ok(Default::default())
    }
}

impl Reader for Incremental {
    #[inline]
    fn current_offset(&self) -> VarInt {
        self.current_offset
    }

    #[inline]
    fn final_offset(&self) -> Option<VarInt> {
        self.final_offset
    }
}

pub struct WithChunk<'a, C: Storage> {
    incremental: &'a mut Incremental,
    chunk: &'a mut C,
}

impl<'a, C: Storage> WithChunk<'a, C> {
    #[inline]
    pub fn set_fin(&mut self) -> Result<&mut Self, buffer::Error> {
        let final_offset = self
            .incremental
            .current_offset
            .checked_add_usize(self.buffered_len())
            .ok_or(buffer::Error::OutOfRange)?;

        // make sure the final length doesn't change
        if let Some(current) = self.incremental.final_offset {
            ensure!(final_offset == current, Err(buffer::Error::InvalidFin));
        }

        self.incremental.final_offset = Some(final_offset);

        Ok(self)
    }
}

impl<'a, C: Storage> Storage for WithChunk<'a, C> {
    type Error = C::Error;

    #[inline]
    fn buffered_len(&self) -> usize {
        self.chunk.buffered_len()
    }

    #[inline]
    fn read_chunk(&mut self, watermark: usize) -> Result<storage::Chunk, Self::Error> {
        let chunk = self.chunk.read_chunk(watermark)?;
        self.incremental.current_offset += chunk.len();
        Ok(chunk)
    }

    #[inline]
    fn partial_copy_into<Dest: crate::buffer::writer::Storage>(
        &mut self,
        dest: &mut Dest,
    ) -> Result<storage::Chunk, Self::Error> {
        let len = self.buffered_len().min(dest.remaining_capacity());
        let chunk = self.chunk.partial_copy_into(dest)?;
        self.incremental.current_offset += len;
        Ok(chunk)
    }

    #[inline]
    fn copy_into<Dest: crate::buffer::writer::Storage>(
        &mut self,
        dest: &mut Dest,
    ) -> Result<(), Self::Error> {
        let len = self.buffered_len().min(dest.remaining_capacity());
        self.chunk.copy_into(dest)?;
        self.incremental.current_offset += len;
        Ok(())
    }
}

impl<'a, C: Storage> Reader for WithChunk<'a, C> {
    #[inline]
    fn current_offset(&self) -> VarInt {
        self.incremental.current_offset()
    }

    #[inline]
    fn final_offset(&self) -> Option<VarInt> {
        self.incremental.final_offset()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn incremental_test() {
        let mut incremental = Incremental::default();

        assert_eq!(incremental.current_offset(), VarInt::ZERO);
        assert_eq!(incremental.final_offset, None);
        assert_eq!(incremental.buffered_len(), 0);

        {
            let mut chunk: &[u8] = &[1, 2, 3, 4];
            let mut with_chunk = incremental.with_chunk(&mut chunk, false).unwrap();

            assert_eq!(with_chunk.buffered_len(), 4);

            let mut dest: &mut [u8] = &mut [0; 4];
            let trailing_chunk = with_chunk.partial_copy_into(&mut dest).unwrap();
            assert_eq!(&*trailing_chunk, &[1, 2, 3, 4]);

            assert_eq!(with_chunk.buffered_len(), 0);
        }

        assert_eq!(incremental.current_offset(), VarInt::from_u8(4));
        assert_eq!(incremental.buffered_len(), 0);
    }
}
