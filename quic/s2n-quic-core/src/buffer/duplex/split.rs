// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

use crate::buffer::{
    reader::Reader,
    writer::{Storage, Writer},
    Duplex, Error,
};

pub struct Split<'a, C: Storage, D: Duplex<Error = core::convert::Infallible> + ?Sized> {
    chunk: &'a mut C,
    duplex: &'a mut D,
}

impl<'a, C: Storage, D: Duplex<Error = core::convert::Infallible> + ?Sized> Split<'a, C, D> {
    #[inline]
    pub fn new(chunk: &'a mut C, duplex: &'a mut D) -> Self {
        Self { chunk, duplex }
    }
}

impl<'a, C: Storage, D: Duplex<Error = core::convert::Infallible> + ?Sized> Writer
    for Split<'a, C, D>
{
    #[inline]
    fn copy_from<R: Reader>(&mut self, reader: &mut R) -> Result<(), Error<R::Error>> {
        let initial_offset = reader.current_offset();
        let final_offset = reader.final_offset();
        let is_contiguous = initial_offset == self.duplex.current_offset();

        {
            // if the chunk specializes writing zero-copy Bytes/BytesMut, then just write to the
            // receive buffer, since that's what it stores
            let mut should_delegate = C::SPECIALIZES_BYTES || C::SPECIALIZES_BYTES_MUT;

            // if this packet is non-contiguous, then delegate to the wrapped writer
            should_delegate |= !is_contiguous;

            // if the chunk doesn't have any remaining capacity, then delegate
            should_delegate |= !self.chunk.has_remaining_capacity();

            if should_delegate {
                self.duplex.copy_from(reader)?;

                if !self.duplex.buffer_is_empty() && self.chunk.has_remaining_capacity() {
                    self.duplex
                        .copy_into(self.chunk)
                        .expect("duplex error is infallible");
                }

                return Ok(());
            }
        }

        debug_assert!(self.chunk.has_remaining_capacity());

        reader.copy_into(self.chunk)?;
        let write_len = initial_offset - reader.current_offset();

        self.duplex
            .skip(write_len, final_offset)
            .map_err(Error::mapped)?;

        if !reader.buffer_is_empty() {
            self.duplex.copy_from(reader)?;
        }

        Ok(())
    }
}
