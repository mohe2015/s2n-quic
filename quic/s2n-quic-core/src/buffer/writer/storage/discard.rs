// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

#[derive(Clone, Copy, Debug, Default)]
pub struct Discard;

impl super::Storage for Discard {
    #[inline]
    fn put_slice(&mut self, bytes: &[u8]) {
        let _ = bytes;
    }

    #[inline]
    fn remaining_capacity(&self) -> usize {
        usize::MAX
    }
}
