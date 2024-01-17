// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

use super::{Chunk, Storage};
use crate::assume;
use core::cmp::Ordering;

pub struct IoSlice<'a, T> {
    len: usize,
    head: &'a [u8],
    buf: &'a [T],
}

impl<'a, T> IoSlice<'a, T>
where
    T: core::ops::Deref<Target = [u8]>,
{
    #[inline]
    pub fn new(buf: &'a [T]) -> Self {
        let mut len = 0;

        let mut first_non_empty = None;
        for (idx, buf) in buf.iter().enumerate() {
            len += buf.len();
            if !buf.is_empty() && first_non_empty.is_none() {
                first_non_empty = Some(idx);
            }
        }

        if let Some(idx) = first_non_empty {
            let buf = &buf[idx..];

            unsafe {
                assume!(!buf.is_empty());
            }

            let mut slice = Self {
                len,
                head: &[],
                buf,
            };
            slice.advance_buf_once();
            slice
        } else {
            Self {
                len: 0,
                head: &[],
                buf: &[],
            }
        }
    }

    #[inline(always)]
    fn advance_buf(&mut self) {
        while self.head.is_empty() && !self.buf.is_empty() {
            self.advance_buf_once();
        }
    }

    #[inline(always)]
    fn advance_buf_once(&mut self) {
        let (head, tail) = self.buf.split_at(1);
        self.head = &head[0][..];
        self.buf = tail;
    }

    #[inline]
    fn sub_len(&mut self, len: usize) {
        unsafe {
            assume!(self.len >= len);
        }
        self.set_len(self.len - len);
    }

    #[inline]
    fn set_len(&mut self, len: usize) {
        if cfg!(debug_assertions) {
            let mut computed = self.head.len();
            for buf in self.buf.iter() {
                computed += buf.len();
            }
            assert_eq!(len, computed);
        }
        self.len = len;
    }
}

impl<'a, T> bytes::Buf for IoSlice<'a, T>
where
    T: core::ops::Deref<Target = [u8]>,
{
    #[inline]
    fn remaining(&self) -> usize {
        self.len
    }

    #[inline]
    fn chunk(&self) -> &[u8] {
        self.head
    }

    #[inline]
    fn advance(&mut self, mut cnt: usize) {
        assert!(cnt <= self.len);
        let new_len = self.len - cnt;

        if new_len == 0 {
            self.head = &[];
            self.buf = &[];
            self.set_len(new_len);
            return;
        }

        while cnt > 0 {
            let len = self.head.len().min(cnt);
            cnt -= len;

            if len >= self.head.len() {
                unsafe {
                    assume!(!self.buf.is_empty());
                }

                self.head = &[];
                self.advance_buf();
                continue;
            }

            self.head = &self.head[len..];
            break;
        }

        self.set_len(new_len);
    }
}

impl<'a, T> Storage for IoSlice<'a, T>
where
    T: core::ops::Deref<Target = [u8]>,
{
    type Error = core::convert::Infallible;

    #[inline]
    fn buffered_len(&self) -> usize {
        self.len
    }

    #[inline]
    fn read_chunk(&mut self, watermark: usize) -> Result<Chunk, Self::Error> {
        // we only have one chunk left so do the happy path
        if self.buf.is_empty() {
            let len = self.head.len().min(watermark);
            let (head, tail) = self.head.split_at(len);
            self.head = tail;
            self.set_len(tail.len());
            return Ok(head.into());
        }

        // head can be returned and we need to take the next buf entry
        if self.head.len() >= watermark {
            let head = self.head;
            self.head = &[];
            unsafe {
                assume!(!self.buf.is_empty());
            }
            self.advance_buf();
            self.sub_len(head.len());
            return Ok(head.into());
        }

        // we just need to split off the current head and return it
        let (head, tail) = self.head.split_at(watermark);
        self.head = tail;
        self.sub_len(head.len());
        Ok(head.into())
    }

    #[inline]
    fn partial_copy_into<Dest: crate::buffer::writer::Storage>(
        &mut self,
        dest: &mut Dest,
    ) -> Result<Chunk, Self::Error> {
        ensure!(dest.has_remaining_capacity(), Ok(Default::default()));

        loop {
            // we only have one chunk left so do the happy path
            if self.buf.is_empty() {
                let len = self.head.len().min(dest.remaining_capacity());
                let (head, tail) = self.head.split_at(len);
                self.head = tail;
                self.set_len(tail.len());
                return Ok(head.into());
            }

            match self.head.len().cmp(&dest.remaining_capacity()) {
                // head needs to be copied into dest and we need to take the next buf entry
                Ordering::Less => {
                    let len = self.head.len();
                    dest.put_slice(self.head);
                    self.head = &[];
                    unsafe {
                        assume!(!self.buf.is_empty());
                    }
                    self.advance_buf();
                    self.sub_len(len);
                    continue;
                }
                // head can be returned and we need to take the next buf entry
                Ordering::Equal => {
                    let head = self.head;
                    self.head = &[];
                    unsafe {
                        assume!(!self.buf.is_empty());
                    }
                    self.advance_buf();
                    self.sub_len(head.len());
                    return Ok(head.into());
                }
                // we just need to split off the current head and return it
                Ordering::Greater => {
                    let (head, tail) = self.head.split_at(dest.remaining_capacity());
                    self.head = tail;
                    self.sub_len(head.len());
                    return Ok(head.into());
                }
            }
        }
    }
}
