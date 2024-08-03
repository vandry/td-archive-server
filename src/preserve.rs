use bytes::{Buf, BufMut};
use prost::encoding::{DecodeContext, WireType};
use prost::DecodeError;
use std::io::Read;
use std::ops::Deref;

use crate::openraildata_pb;

// We accept protos in and produce protos out, but unfortunately Prost!
// drops unknown fields in the deserialisation and reserialisation.
// This is a wrapper for prost::Message that will remember the original
// bytes upon deserialisation and emit the same bytes when the proto is
// serialised again. For our application we only need to emit the same
// identical protos as we ingested, so we deal with the problem of the
// preserved serialised bytes becoming out of sync by just not allowing
// any mutation. Also, we may unfortunately depend on some internals.

// The types that the gRPC clients and servers should be redirected to.
pub type TdQuery = openraildata_pb::TdQuery;
pub type TdFrame = PreservingMessage<openraildata_pb::TdFrame>;

#[derive(Clone, Debug, Default)]
pub struct PreservingMessage<T> {
    decoded: T,
    original_serialised: Vec<u8>,
}

impl<T> Deref for PreservingMessage<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.decoded
    }
}

impl<T: prost::Message + Default> prost::Message for PreservingMessage<T> {
    fn encode_raw<B>(&self, buf: &mut B)
    where
        B: BufMut,
    {
        buf.put_slice(&self.original_serialised);
    }

    fn merge_field<B>(
        &mut self,
        _tag: u32,
        _wire_type: WireType,
        _buf: &mut B,
        _ctx: DecodeContext,
    ) -> Result<(), DecodeError>
    where
        B: Buf,
    {
        panic!("PreservingMessage is immutable");
    }

    fn encoded_len(&self) -> usize {
        self.original_serialised.len()
    }

    fn clear(&mut self) {
        panic!("PreservingMessage is immutable");
    }

    fn decode<B>(buf: B) -> Result<Self, DecodeError>
    where
        B: Buf,
    {
        let mut original_serialised = Vec::with_capacity(buf.remaining());
        buf.reader().read_to_end(&mut original_serialised).unwrap();
        let decoded = T::decode(std::io::Cursor::new(&original_serialised))?;
        Ok(Self {
            decoded,
            original_serialised,
        })
    }
}
