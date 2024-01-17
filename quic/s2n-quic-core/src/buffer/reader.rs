// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

use crate::varint::VarInt;

mod empty;
pub mod incremental;
mod limit;
mod slice;
pub mod storage;

pub use empty::Empty;
pub use incremental::Incremental;
pub use limit::Limit;
pub use slice::Slice;
pub use storage::Storage;

pub trait Reader: Storage {
    /// Returns the currently read offset for the stream
    fn current_offset(&self) -> VarInt;

    /// Returns the final offset for the stream
    fn final_offset(&self) -> Option<VarInt>;

    /// Returns `true` if the reader has the final offset buffered
    #[inline]
    fn has_buffered_fin(&self) -> bool {
        self.final_offset().map_or(false, |fin| {
            let buffered_end = self
                .current_offset()
                .as_u64()
                .saturating_add(self.buffered_len() as u64);
            fin == buffered_end
        })
    }

    /// Returns `true` if the reader is finished producing data
    #[inline]
    fn is_consumed(&self) -> bool {
        self.final_offset()
            .map_or(false, |fin| fin == self.current_offset())
    }

    /// Limits the maximum offset that the caller can read from the reader
    #[inline]
    fn with_max_data(&mut self, max_data: VarInt) -> Limit<Self> {
        let max_buffered_len = max_data.saturating_sub(self.current_offset());
        let max_buffered_len = max_buffered_len.as_u64().min(self.buffered_len() as u64) as usize;
        self.with_limit(max_buffered_len)
    }

    /// Limits the maximum amount of data that the caller can read from the reader
    #[inline]
    fn with_limit(&mut self, max_buffered_len: usize) -> Limit<Self> {
        Limit::new(self, max_buffered_len)
    }

    /// Temporarily clears the buffer for the reader, while preserving the offsets
    #[inline]
    fn with_empty_buffer(&self) -> Empty<Self> {
        Empty::new(self)
    }
}
