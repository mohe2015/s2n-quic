// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

mod buf;
mod bytes;
mod chunk;
mod full_copy;
mod io_slice;
mod slice;

#[cfg(test)]
mod tests;

pub use buf::Buf;
pub use chunk::Chunk;
pub use full_copy::FullCopy;
pub use io_slice::IoSlice;

pub trait Storage {
    type Error;

    /// Returns the length of the chunk
    fn buffered_len(&self) -> usize;

    /// Returns if the chunk is empty
    #[inline]
    fn buffer_is_empty(&self) -> bool {
        self.buffered_len() == 0
    }

    /// Reads the current contiguous chunk
    fn read_chunk(&mut self, watermark: usize) -> Result<Chunk<'_>, Self::Error>;

    /// Copies the reader into `dest`, with a trailing chunk of bytes.
    ///
    /// Implementations should either fill the `dest` completely or exhaust the buffered data.
    ///
    /// The storage may optionally return a `Chunk`, which can be used by the caller to defer
    /// copying the trailing chunk until later.
    fn partial_copy_into<Dest>(&mut self, dest: &mut Dest) -> Result<Chunk<'_>, Self::Error>
    where
        Dest: crate::buffer::writer::Storage;

    /// Forces the entire reader to be copied, even when calling `partial_copy_into`.
    ///
    /// The returned `Chunk` from `partial_copy_into` will always be empty.
    #[inline]
    fn full_copy(&mut self) -> FullCopy<Self> {
        FullCopy::new(self)
    }

    /// Copies the reader into `dest`.
    ///
    /// Implementations should either fill the `dest` completely or exhaust the buffered data.
    #[inline]
    fn copy_into<Dest>(&mut self, dest: &mut Dest) -> Result<(), Self::Error>
    where
        Dest: crate::buffer::writer::Storage,
    {
        let mut chunk = self.partial_copy_into(dest)?;
        let _: Result<(), core::convert::Infallible> = chunk.copy_into(dest);
        Ok(())
    }
}
