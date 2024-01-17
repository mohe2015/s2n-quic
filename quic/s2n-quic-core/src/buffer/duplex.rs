// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

use super::{Error, Reader, Writer};
use crate::varint::VarInt;

mod split;

pub use split::Split;

pub trait Duplex: Reader + Writer {
    fn skip(&mut self, len: VarInt, final_offset: Option<VarInt>) -> Result<(), Error>;
}
