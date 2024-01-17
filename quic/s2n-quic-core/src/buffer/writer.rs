// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

pub mod storage;

pub use storage::Storage;

pub trait Writer {
    fn copy_from<R: super::Reader>(&mut self, reader: &mut R)
        -> Result<(), super::Error<R::Error>>;
}
