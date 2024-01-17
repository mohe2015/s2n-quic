// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

//! This module contains data structures for buffering incoming streams.

use super::Error;
use crate::{buffer, varint::VarInt};
use alloc::collections::{vec_deque, VecDeque};
use bytes::BytesMut;

mod probe;
mod request;
mod slot;

#[cfg(test)]
mod tests;

use request::Request;
use slot::Slot;

/// The default buffer size for slots that the [`Reassembler`] uses.
///
/// This value was picked as it is typically used for the default memory page size.
const MIN_BUFFER_ALLOCATION_SIZE: usize = 4096;

/// The value used for when the final size is unknown.
///
/// By using `u64::MAX` we don't have to special case any of the logic. Also note that the actual
/// max size of any stream is a `VarInt::MAX` so this isn't a valid value.
const UNKNOWN_FINAL_SIZE: u64 = u64::MAX;

//= https://www.rfc-editor.org/rfc/rfc9000#section-2.2
//# Endpoints MUST be able to deliver stream data to an application as an
//# ordered byte-stream.

/// `Reassembler` is a buffer structure for combining chunks of bytes in an
/// ordered stream, which might arrive out of order.
///
/// `Reassembler` will accumulate the bytes, and provide them to its users
/// once a contiguous range of bytes at the current position of the stream has
/// been accumulated.
///
/// `Reassembler` is optimized for minimizing memory allocations and for
/// offering it's users chunks of sizes that minimize call overhead.
///
/// If data is received in smaller chunks, only the first chunk will trigger a
/// memory allocation. All other chunks can be copied into the already allocated
/// region.
///
/// When users want to consume data from the buffer, the consumable part of the
/// internal receive buffer is split off and passed back to the caller. Due to
/// this chunk being a view onto a reference-counted internal buffer of type
/// [`BytesMut`] this is also efficient and does not require additional memory
/// allocation or copy.
///
/// ## Usage
///
/// ```rust
/// use s2n_quic_core::buffer::Reassembler;
///
/// let mut buffer = Reassembler::new();
///
/// // write a chunk of bytes at offset 4, which can not be consumed yet
/// assert!(buffer.write_at(4u32.into(), &[4, 5, 6, 7]).is_ok());
/// assert_eq!(0, buffer.len());
/// assert_eq!(None, buffer.pop());
///
/// // write a chunk of bytes at offset 0, which allows for consumption
/// assert!(buffer.write_at(0u32.into(), &[0, 1, 2, 3]).is_ok());
/// assert_eq!(8, buffer.len());
///
/// // Pop chunks. Since they all fitted into a single internal buffer,
/// // they will be returned in combined fashion.
/// assert_eq!(&[0u8, 1, 2, 3, 4, 5, 6, 7], &buffer.pop().unwrap()[..]);
/// ```
#[derive(Debug, PartialEq)]
pub struct Reassembler {
    slots: VecDeque<Slot>,
    start_offset: u64,
    max_recv_offset: u64,
    final_offset: u64,
}

impl Default for Reassembler {
    fn default() -> Self {
        Self::new()
    }
}

impl Reassembler {
    /// Creates a new `Reassembler`
    pub fn new() -> Reassembler {
        Reassembler {
            slots: VecDeque::new(),
            start_offset: 0,
            max_recv_offset: 0,
            final_offset: UNKNOWN_FINAL_SIZE,
        }
    }

    /// Returns true if the buffer has completely been written to and the final size is known
    #[inline]
    pub fn is_writing_complete(&self) -> bool {
        self.final_size()
            .map_or(false, |len| self.total_received_len() == len)
    }

    /// Returns true if the buffer has completely been read and the final size is known
    #[inline]
    pub fn is_reading_complete(&self) -> bool {
        self.final_size()
            .map_or(false, |len| self.start_offset == len)
    }

    /// Returns the final size of the stream, if known
    #[inline]
    pub fn final_size(&self) -> Option<u64> {
        if self.final_offset == UNKNOWN_FINAL_SIZE {
            None
        } else {
            Some(self.final_offset)
        }
    }

    /// Returns the amount of bytes available for reading.
    /// This equals the amount of data that is stored in contiguous fashion at
    /// the start of the buffer.
    #[inline]
    pub fn len(&self) -> usize {
        self.report().0
    }

    /// Returns true if no bytes are available for reading
    #[inline]
    pub fn is_empty(&self) -> bool {
        if let Some(slot) = self.slots.front() {
            !slot.is_occupied(self.start_offset)
        } else {
            true
        }
    }

    /// Returns the number of bytes and chunks available for consumption
    #[inline]
    pub fn report(&self) -> (usize, usize) {
        let mut bytes = 0;
        let mut chunks = 0;
        for chunk in self.iter() {
            bytes += chunk.len();
            chunks += 1;
        }
        (bytes, chunks)
    }

    /// Pushes a slice at a certain offset
    #[inline]
    pub fn write_at(&mut self, offset: VarInt, data: &[u8]) -> Result<(), Error> {
        // create a request
        let request = Request::new(offset, data)?;
        self.write_request(request)?;
        Ok(())
    }

    /// Pushes a slice at a certain offset, which is the end of the buffer
    #[inline]
    pub fn write_at_fin(&mut self, offset: VarInt, data: &[u8]) -> Result<(), Error> {
        // create a request
        let request = Request::new(offset, data)?;

        // compute the final offset for the fin request
        let final_offset = request.end_exclusive();

        // make sure if we previously saw a final size that they still match
        //= https://www.rfc-editor.org/rfc/rfc9000#section-4.5
        //# Once a final size for a stream is known, it cannot change.  If a
        //# RESET_STREAM or STREAM frame is received indicating a change in the
        //# final size for the stream, an endpoint SHOULD respond with an error
        //# of type FINAL_SIZE_ERROR; see Section 11 for details on error
        //# handling.
        if let Some(final_size) = self.final_size() {
            ensure!(final_size == final_offset, Err(Error::InvalidFin));
        }

        // make sure that we didn't see any previous chunks greater than the final size
        ensure!(self.max_recv_offset <= final_offset, Err(Error::InvalidFin));

        self.final_offset = final_offset;

        self.write_request(request)?;

        Ok(())
    }

    #[inline]
    fn write_request(&mut self, request: Request) -> Result<(), Error> {
        // trim off any data that we've already read
        let (_, request) = request.split(self.start_offset);
        // trim off any data that exceeds our final length
        let (mut request, excess) = request.split(self.final_offset);

        // make sure the request isn't trying to write beyond the final size
        //= https://www.rfc-editor.org/rfc/rfc9000#section-4.5
        //# Once a final size for a stream is known, it cannot change.  If a
        //# RESET_STREAM or STREAM frame is received indicating a change in the
        //# final size for the stream, an endpoint SHOULD respond with an error
        //# of type FINAL_SIZE_ERROR; see Section 11 for details on error
        //# handling.
        ensure!(excess.is_empty(), Err(Error::InvalidFin));

        // if the request is empty we're done
        ensure!(!request.is_empty(), Ok(()));

        // record the maximum offset that we've seen
        self.max_recv_offset = self.max_recv_offset.max(request.end_exclusive());

        // start from the back with the assumption that most data arrives in order
        for mut idx in (0..self.slots.len()).rev() {
            unsafe {
                assume!(self.slots.len() > idx);
            }
            let slot = &mut self.slots[idx];

            let slot::Outcome { lower, mid, upper } = slot.try_write(request);

            // if this slot was completed, we should try and unsplit with the next slot
            if slot.is_full() {
                let current_block =
                    Self::align_offset(slot.start(), Self::allocation_size(slot.start()));
                let end = slot.end();

                if let Some(next) = self.slots.get(idx + 1) {
                    let next_block =
                        Self::align_offset(next.start(), Self::allocation_size(next.start()));

                    if next.start() == end && current_block == next_block {
                        unsafe {
                            assume!(self.slots.len() > idx + 1);
                        }
                        if let Some(next) = self.slots.remove(idx + 1) {
                            self.slots[idx].unsplit(next);
                        }
                    }
                }
            }

            idx += 1;
            self.allocate_request(idx, upper);

            if let Some(mid) = mid {
                self.insert(idx, mid);
            }

            request = lower;

            if request.is_empty() {
                break;
            }
        }

        self.allocate_request(0, request);

        self.invariants();

        Ok(())
    }

    /// Advances the read and write cursors and discards any held data
    ///
    /// This can be used for copy-avoidance applications where a packet is received in order and
    /// doesn't need to be stored temporarily for future packets to unblock the stream.
    #[inline]
    pub fn skip(&mut self, len: VarInt) -> Result<(), Error> {
        // zero-length skip is a no-op
        ensure!(len > VarInt::ZERO, Ok(()));

        let new_start_offset = self
            .start_offset
            .checked_add(len.as_u64())
            .ok_or(Error::OutOfRange)?;

        if let Some(final_size) = self.final_size() {
            ensure!(final_size >= new_start_offset, Err(Error::InvalidFin));
        }

        // record the maximum offset that we've seen
        self.max_recv_offset = self.max_recv_offset.max(new_start_offset);

        // update the current start offset
        self.start_offset = new_start_offset;

        // clear out the slots to the new start offset
        while let Some(mut slot) = self.slots.pop_front() {
            // the new offset consumes the slot so drop and continue
            if slot.end_allocated() < new_start_offset {
                continue;
            }

            match new_start_offset.checked_sub(slot.start()) {
                None | Some(0) => {
                    // the slot starts after/on the new offset so put it back and break out
                    self.slots.push_front(slot);
                }
                Some(len) => {
                    // the slot overlaps with the new boundary so modify it and put it back if
                    // needed
                    slot.skip(len);

                    if !slot.should_drop() {
                        self.slots.push_front(slot);
                    }
                }
            }

            break;
        }

        self.invariants();

        Ok(())
    }

    /// Iterates over all of the chunks waiting to be received
    #[inline]
    pub fn iter(&self) -> impl Iterator<Item = &[u8]> {
        Iter::new(self)
    }

    /// Drains all of the currently available chunks
    #[inline]
    pub fn drain(&mut self) -> impl Iterator<Item = BytesMut> + '_ {
        Drain { inner: self }
    }

    /// Pops a buffer from the front of the receive queue if available
    #[inline]
    pub fn pop(&mut self) -> Option<BytesMut> {
        self.pop_transform(|buffer, is_final_offset| {
            let chunk = if is_final_offset || buffer.len() == buffer.capacity() {
                core::mem::take(buffer)
            } else {
                buffer.split()
            };
            let len = chunk.len();
            (chunk, len)
        })
    }

    /// Pops a buffer from the front of the receive queue, who's length is always guaranteed to be
    /// less than the provided `watermark`.
    #[inline]
    pub fn pop_watermarked(&mut self, watermark: usize) -> Option<BytesMut> {
        self.pop_transform(|buffer, is_final_offset| {
            // make sure the buffer doesn't exceed the watermark
            let watermark = watermark.min(buffer.len());

            // if the watermark is 0 then don't needlessly increment refcounts
            ensure!(watermark > 0, (BytesMut::new(), 0));

            if watermark == buffer.len() && is_final_offset {
                return (core::mem::take(buffer), watermark);
            }

            (buffer.split_to(watermark), watermark)
        })
    }

    /// Pops a buffer from the front of the receive queue as long as the `transform` function returns a
    /// non-empty buffer.
    #[inline]
    fn pop_transform<F: FnOnce(&mut BytesMut, bool) -> (O, usize), O>(
        &mut self,
        transform: F,
    ) -> Option<O> {
        let slot = self.slots.front_mut()?;

        // make sure the slot has some data
        ensure!(slot.is_occupied(self.start_offset), None);

        let is_final_offset = self.final_offset == slot.end();
        let buffer = slot.data_mut();

        let (out, len) = transform(buffer, is_final_offset);

        // filter out empty buffers
        ensure!(len > 0, None);

        slot.add_start(len);

        if slot.should_drop() {
            // remove empty buffers
            self.slots.pop_front();
        }

        probe::pop(self.start_offset, len);

        self.start_offset += len as u64;

        self.invariants();

        Some(out)
    }

    /// Returns the amount of data that had already been consumed from the
    /// receive buffer.
    #[inline]
    pub fn consumed_len(&self) -> u64 {
        self.start_offset
    }

    /// Returns the total amount of contiguous received data.
    ///
    /// This includes the already consumed data as well as the data that is still
    /// buffered and available for consumption.
    #[inline]
    pub fn total_received_len(&self) -> u64 {
        let mut offset = self.start_offset;

        for slot in &self.slots {
            ensure!(slot.is_occupied(offset), offset);
            offset = slot.end();
        }

        offset
    }

    /// Resets the receive buffer.
    ///
    /// This will drop all previously received data.
    #[inline]
    pub fn reset(&mut self) {
        self.slots.clear();
        self.start_offset = Default::default();
        self.max_recv_offset = 0;
        self.final_offset = u64::MAX;
    }

    #[inline(always)]
    fn insert(&mut self, idx: usize, slot: Slot) {
        if self.slots.len() < idx {
            debug_assert_eq!(self.slots.len() + 1, idx);
            self.slots.push_back(slot);
        } else {
            self.slots.insert(idx, slot);
        }
    }

    #[inline]
    fn allocate_request(&mut self, mut idx: usize, mut request: Request) {
        ensure!(!request.is_empty());

        // if this is a fin request and the write is under the allocation size, then no need to
        // do a full allocation that doesn't end up getting used.
        if request.end_exclusive() == self.final_offset {
            while !request.is_empty() {
                let start = request.start();
                let mut size = Self::allocation_size(start);
                let offset = Self::align_offset(start, size);

                let size_candidate = (start - offset) as usize + request.len();
                if size_candidate < size {
                    size = size_candidate;
                }

                // set the current request to the upper slot and loop
                request = self.allocate_slot(&mut idx, request, offset, size);
            }
            return;
        }

        while !request.is_empty() {
            let start = request.start();
            let size = Self::allocation_size(start);
            let offset = Self::align_offset(start, size);
            // set the current request to the upper slot and loop
            request = self.allocate_slot(&mut idx, request, offset, size);
        }
    }

    #[inline]
    fn allocate_slot<'a>(
        &mut self,
        idx: &mut usize,
        request: Request<'a>,
        mut offset: u64,
        mut size: usize,
    ) -> Request<'a> {
        // don't allocate for data we've already consumed
        if let Some(diff) = self.start_offset.checked_sub(offset) {
            debug_assert!(
                request.start() >= self.start_offset,
                "requests should be split before allocating slots"
            );
            offset = self.start_offset;
            size -= diff as usize;
        }

        let buffer = BytesMut::with_capacity(size);

        let end = offset + size as u64;
        let mut slot = Slot::new(offset, end, buffer);

        let slot::Outcome { lower, mid, upper } = slot.try_write(request);

        unsafe {
            assume!(lower.is_empty(), "lower requests should always be empty");
        }

        // first insert the newly-created Slot
        debug_assert!(!slot.should_drop());
        self.insert(*idx, slot);
        *idx += 1;

        // check if we have a mid-slot and insert that as well
        if let Some(mid) = mid {
            debug_assert!(!mid.should_drop());
            self.insert(*idx, mid);
            *idx += 1;
        }

        // return the upper request if we need to allocate more
        upper
    }

    /// Aligns an offset to a certain alignment size
    #[inline(always)]
    fn align_offset(offset: u64, alignment: usize) -> u64 {
        unsafe {
            assume!(alignment > 0);
        }
        (offset / (alignment as u64)) * (alignment as u64)
    }

    /// Returns the desired allocation size for the given offset
    ///
    /// The allocation size gradually increases as the offset increases. This is under
    /// the assumption that streams that receive a lot of data will continue to receive
    /// a lot of data.
    ///
    /// The current table is as follows:
    ///
    /// | offset         | allocation size |
    /// |----------------|-----------------|
    /// | 0              | 4096            |
    /// | 65536          | 16384           |
    /// | 262144         | 32768           |
    /// | >=1048575      | 65536           |
    #[inline(always)]
    fn allocation_size(offset: u64) -> usize {
        for pow in (2..=4).rev() {
            let mult = 1 << pow;
            let square = mult * mult;
            let min_offset = (MIN_BUFFER_ALLOCATION_SIZE * square) as u64;
            let allocation_size = MIN_BUFFER_ALLOCATION_SIZE * mult;

            if offset >= min_offset {
                return allocation_size;
            }
        }

        MIN_BUFFER_ALLOCATION_SIZE
    }

    #[inline(always)]
    fn invariants(&self) {
        if cfg!(debug_assertions) {
            assert_eq!(
                self.total_received_len(),
                self.consumed_len() + self.len() as u64
            );

            let (actual_len, chunks) = self.report();

            assert_eq!(actual_len == 0, self.is_empty());
            assert_eq!(self.iter().count(), chunks);

            let mut prev_end = self.start_offset;

            for slot in &self.slots {
                assert!(slot.start() >= prev_end, "{self:#?}");
                assert!(!slot.should_drop(), "slot range should be non-empty");
                prev_end = slot.end_allocated();
            }
        }
    }
}

pub struct Iter<'a> {
    prev_end: u64,
    inner: vec_deque::Iter<'a, Slot>,
}

impl<'a> Iter<'a> {
    #[inline]
    fn new(buffer: &'a Reassembler) -> Self {
        Self {
            prev_end: buffer.start_offset,
            inner: buffer.slots.iter(),
        }
    }
}

impl<'a> Iterator for Iter<'a> {
    type Item = &'a [u8];

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        let slot = self.inner.next()?;

        ensure!(slot.is_occupied(self.prev_end), None);

        self.prev_end = slot.end();
        Some(slot.as_slice())
    }
}

pub struct Drain<'a> {
    inner: &'a mut Reassembler,
}

impl<'a> Iterator for Drain<'a> {
    type Item = BytesMut;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.pop()
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.inner.slots.len();
        (len, Some(len))
    }
}

impl buffer::reader::Storage for Reassembler {
    type Error = core::convert::Infallible;

    #[inline]
    fn buffered_len(&self) -> usize {
        self.len()
    }

    #[inline]
    fn buffer_is_empty(&self) -> bool {
        self.is_empty()
    }

    #[inline]
    fn read_chunk(
        &mut self,
        watermark: usize,
    ) -> Result<buffer::reader::storage::Chunk, Self::Error> {
        if let Some(chunk) = self.pop_watermarked(watermark) {
            return Ok(chunk.into());
        }

        Ok(Default::default())
    }

    #[inline]
    fn partial_copy_into<Dest: buffer::writer::Storage>(
        &mut self,
        dest: &mut Dest,
    ) -> Result<buffer::reader::storage::Chunk, Self::Error> {
        let mut prev = BytesMut::new();

        loop {
            let remaining = dest.remaining_capacity();
            // ensure we have enough capacity in the destination buf
            ensure!(remaining > 0, Ok(Default::default()));

            match self.pop_watermarked(remaining) {
                Some(chunk) => {
                    let mut prev = core::mem::replace(&mut prev, chunk);
                    if !prev.is_empty() {
                        let _: Result<(), core::convert::Infallible> = prev.copy_into(dest);
                    }
                }
                None if prev.is_empty() => {
                    return Ok(Default::default());
                }
                None => {
                    return Ok(prev.into());
                }
            }
        }
    }

    #[inline]
    fn copy_into<Dest: buffer::writer::Storage>(
        &mut self,
        dest: &mut Dest,
    ) -> Result<(), Self::Error> {
        loop {
            let remaining = dest.remaining_capacity();
            // ensure we have enough capacity in the destination buf
            ensure!(remaining > 0, Ok(()));

            let transform = |buffer: &mut BytesMut, _is_final_offset| {
                let len = buffer.len().min(remaining);
                let _: Result<(), core::convert::Infallible> = buffer.copy_into(dest);
                ((), len)
            };

            if self.pop_transform(transform).is_none() {
                return Ok(());
            }
        }
    }
}

impl buffer::reader::Reader for Reassembler {
    #[inline]
    fn current_offset(&self) -> VarInt {
        unsafe {
            // SAFETY: offset will always fit into a VarInt
            VarInt::new_unchecked(self.start_offset)
        }
    }

    #[inline]
    fn final_offset(&self) -> Option<VarInt> {
        self.final_size().map(|v| unsafe {
            // SAFETY: offset will always fit into a VarInt
            VarInt::new_unchecked(v)
        })
    }
}

impl buffer::writer::Writer for Reassembler {
    #[inline]
    fn copy_from<R: buffer::Reader>(
        &mut self,
        reader: &mut R,
    ) -> Result<(), buffer::Error<R::Error>> {
        let final_offset = reader.final_offset();

        // optimize for the case where the stream consists of a single chunk
        if let Some(final_offset) = final_offset {
            let mut is_single_chunk_stream = true;

            let offset = reader.current_offset();

            // the reader is starting at the beginning of the stream
            is_single_chunk_stream &= offset == VarInt::ZERO;
            // the reader has buffered the final offset
            is_single_chunk_stream &= reader.has_buffered_fin();
            // no data has been consumed from the Reassembler
            is_single_chunk_stream &= self.consumed_len() == 0;
            // we aren't tracking any slots
            is_single_chunk_stream &= self.slots.is_empty();

            if is_single_chunk_stream {
                let payload_len = reader.buffered_len();
                let end = final_offset.as_u64();

                // don't allocate anything if we don't need to
                if payload_len == 0 {
                    let chunk = reader.read_chunk(0)?;
                    debug_assert!(chunk.is_empty());
                } else {
                    let mut data = BytesMut::with_capacity(payload_len);

                    // copy the whole thing into `data`
                    reader.copy_into(&mut data)?;

                    self.slots.push_back(Slot::new(offset.as_u64(), end, data));
                };

                // update the final offset after everything was read correctly
                self.final_offset = end;
                self.invariants();

                return Ok(());
            }
        }

        // TODO add better support for copy avoidance by iterating to the appropriate slot and
        // copying into that, if possible

        // fall back to copying individual chunks into the receive buffer
        let mut first_write = true;
        loop {
            let offset = reader.current_offset();
            let chunk = reader.read_chunk(usize::MAX)?;

            // Record the final size before writing to avoid excess allocation. This also needs to
            // happen after we read the first chunk in case there are errors.
            if first_write {
                if let Some(offset) = final_offset {
                    self.write_at_fin(offset, &[])
                        .map_err(buffer::Error::mapped)?;
                }
            }

            // TODO maybe specialize on BytesMut chunks? - for now we'll just treat them as
            // slices

            self.write_at(offset, &chunk)
                .map_err(buffer::Error::mapped)?;

            first_write = false;

            if reader.buffer_is_empty() {
                break;
            }
        }

        Ok(())
    }
}

impl buffer::Duplex for Reassembler {
    #[inline]
    fn skip(&mut self, len: VarInt, final_offset: Option<VarInt>) -> Result<(), Error> {
        if let Some(offset) = final_offset {
            self.write_at_fin(offset, &[])?;
        }

        (*self).skip(len)?;

        Ok(())
    }
}
