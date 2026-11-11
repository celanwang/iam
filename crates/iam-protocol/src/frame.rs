use crate::error::{ProtocolErrorKind, Result};
use crate::msg_type::MsgType;
use bytes::{BufMut, BytesMut};

pub const HEADER_LEN: usize = 12;
pub const FRAME_HARD_MAX: u32 = 1024 * 1024;
pub const HEAD_HARD_MAX: u32 = 256;
pub const PROTOCOL_VERSION: u8 = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FixedHeader {
    pub version: u8,
    pub msg_type: u8,
    pub flags: u16,
    pub head_len: u32,
    pub body_len: u32,
}

impl FixedHeader {
    pub fn new(msg_type: MsgType, head_len: u32, body_len: u32) -> Self {
        Self {
            version: PROTOCOL_VERSION,
            msg_type: msg_type as u8,
            flags: 0,
            head_len,
            body_len,
        }
    }

    pub fn encode(&self, dst: &mut impl BufMut) {
        dst.put_u8(self.version);
        dst.put_u8(self.msg_type);
        dst.put_u16(self.flags);
        dst.put_u32(self.head_len);
        dst.put_u32(self.body_len);
    }

    pub fn decode(src: &[u8]) -> Result<Self> {
        if src.len() < HEADER_LEN {
            return Err(ProtocolErrorKind::MalformedFrame("short header"));
        }
        let version = src[0];
        let msg_type = src[1];
        let flags = u16::from_be_bytes([src[2], src[3]]);
        let head_len = u32::from_be_bytes([src[4], src[5], src[6], src[7]]);
        let body_len = u32::from_be_bytes([src[8], src[9], src[10], src[11]]);
        let header = Self {
            version,
            msg_type,
            flags,
            head_len,
            body_len,
        };
        header.validate_lengths()?;
        Ok(header)
    }

    pub fn validate_lengths(&self) -> Result<()> {
        if self.version != PROTOCOL_VERSION {
            return Err(ProtocolErrorKind::UnsupportedVersion(self.version));
        }
        if self.flags != 0 {
            return Err(ProtocolErrorKind::UnsupportedFlags(self.flags));
        }
        if self.head_len > HEAD_HARD_MAX {
            return Err(ProtocolErrorKind::ResourceLimit("head_len"));
        }
        let payload = self
            .head_len
            .checked_add(self.body_len)
            .ok_or(ProtocolErrorKind::ResourceLimit("length overflow"))?;
        let total = payload
            .checked_add(HEADER_LEN as u32)
            .ok_or(ProtocolErrorKind::ResourceLimit("length overflow"))?;
        if total > FRAME_HARD_MAX {
            return Err(ProtocolErrorKind::ResourceLimit("frame too large"));
        }
        Ok(())
    }

    pub fn payload_len(&self) -> usize {
        (self.head_len + self.body_len) as usize
    }

    pub fn total_len(&self) -> usize {
        HEADER_LEN + self.payload_len()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    pub header: FixedHeader,
    pub head: Vec<u8>,
    pub body: Vec<u8>,
}

#[derive(Debug, Default)]
pub struct FrameDecoder {
    buf: BytesMut,
    failure: Option<ProtocolErrorKind>,
}

impl FrameDecoder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push(&mut self, data: &[u8]) {
        if self.failure.is_some() {
            return;
        }
        if data.len() > FRAME_HARD_MAX as usize - self.buf.len() {
            self.buf.clear();
            self.failure = Some(ProtocolErrorKind::ResourceLimit(
                "receive buffer limit; drain frames before reading more",
            ));
            return;
        }
        self.buf.extend_from_slice(data);
    }

    pub fn next_frame(&mut self) -> Result<Option<Frame>> {
        if let Some(error) = &self.failure {
            return Err(error.clone());
        }
        if self.buf.len() < HEADER_LEN {
            return Ok(None);
        }
        let header = match FixedHeader::decode(&self.buf[..HEADER_LEN]) {
            Ok(header) => header,
            Err(error) => {
                self.failure = Some(error.clone());
                self.buf.clear();
                return Err(error);
            }
        };
        let total = header.total_len();
        if self.buf.len() < total {
            return Ok(None);
        }
        let _ = self.buf.split_to(HEADER_LEN);
        let head = self.buf.split_to(header.head_len as usize).to_vec();
        let body = self.buf.split_to(header.body_len as usize).to_vec();
        Ok(Some(Frame { header, head, body }))
    }

    pub fn finish(self) -> Result<()> {
        if let Some(error) = self.failure {
            return Err(error);
        }
        if self.buf.is_empty() {
            Ok(())
        } else {
            Err(ProtocolErrorKind::MalformedFrame(
                "trailing incomplete frame",
            ))
        }
    }
}
