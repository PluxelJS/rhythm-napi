//! Bounded top-level ISO BMFF probing for progressive MP4 playback.

const MAX_PROBE_BYTES: u64 = 4 * 1024 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum FastStartDecision {
    Pending,
    Progressive,
    ArtifactOnly,
}

#[derive(Debug)]
pub(super) struct FastStartProbe {
    position: u64,
    header: [u8; 16],
    header_len: usize,
    remaining_box_bytes: u64,
    completing_moov: bool,
    seen_ftyp: bool,
    decision: FastStartDecision,
}

impl Default for FastStartProbe {
    fn default() -> Self {
        Self {
            position: 0,
            header: [0; 16],
            header_len: 0,
            remaining_box_bytes: 0,
            completing_moov: false,
            seen_ftyp: false,
            decision: FastStartDecision::Pending,
        }
    }
}

impl FastStartProbe {
    pub(super) fn push(&mut self, mut bytes: &[u8]) -> FastStartDecision {
        while !bytes.is_empty() && self.decision == FastStartDecision::Pending {
            if self.remaining_box_bytes > 0 {
                let consumed = usize::try_from(self.remaining_box_bytes)
                    .unwrap_or(usize::MAX)
                    .min(bytes.len());
                self.position = self.position.saturating_add(consumed as u64);
                self.remaining_box_bytes -= consumed as u64;
                bytes = &bytes[consumed..];
                if self.remaining_box_bytes == 0 && self.completing_moov {
                    self.decision = FastStartDecision::Progressive;
                }
                continue;
            }
            if self.position >= MAX_PROBE_BYTES {
                self.decision = FastStartDecision::ArtifactOnly;
                break;
            }

            let required_header = if self.header_len >= 4 && self.size32() == 1 {
                16
            } else {
                8
            };
            let copied = (required_header - self.header_len).min(bytes.len());
            self.header[self.header_len..self.header_len + copied]
                .copy_from_slice(&bytes[..copied]);
            self.header_len += copied;
            self.position = self.position.saturating_add(copied as u64);
            bytes = &bytes[copied..];
            if self.header_len < required_header || (required_header == 8 && self.size32() == 1) {
                continue;
            }

            let header_bytes = required_header as u64;
            let box_bytes = match self.size32() {
                0 => {
                    self.decision = FastStartDecision::ArtifactOnly;
                    break;
                }
                1 => self.extended_size(),
                size => u64::from(size),
            };
            let box_start = self.position - header_bytes;
            let Some(box_end) = box_start.checked_add(box_bytes) else {
                self.decision = FastStartDecision::ArtifactOnly;
                break;
            };
            if box_bytes < header_bytes || box_end > MAX_PROBE_BYTES {
                self.decision = FastStartDecision::ArtifactOnly;
                break;
            }

            let box_type = [
                self.header[4],
                self.header[5],
                self.header[6],
                self.header[7],
            ];
            self.header_len = 0;
            self.remaining_box_bytes = box_bytes - header_bytes;
            self.completing_moov = box_type == *b"moov";
            match &box_type {
                b"ftyp" => self.seen_ftyp = true,
                b"mdat" => self.decision = FastStartDecision::ArtifactOnly,
                b"moov" if !self.seen_ftyp => self.decision = FastStartDecision::ArtifactOnly,
                b"moov" if self.remaining_box_bytes == 0 => {
                    self.decision = FastStartDecision::Progressive;
                }
                _ => {}
            }
        }
        self.decision
    }

    fn size32(&self) -> u32 {
        u32::from_be_bytes([
            self.header[0],
            self.header[1],
            self.header[2],
            self.header[3],
        ])
    }

    fn extended_size(&self) -> u64 {
        u64::from_be_bytes([
            self.header[8],
            self.header[9],
            self.header[10],
            self.header[11],
            self.header[12],
            self.header[13],
            self.header[14],
            self.header[15],
        ])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn requires_complete_moov_before_mdat_across_chunks() {
        let mut faststart = test_box(*b"ftyp", b"M4A ");
        faststart.extend(test_box(*b"free", b"padding"));
        faststart.extend(test_box(*b"moov", b"metadata"));
        let moov_end = faststart.len();
        faststart.extend(test_box(*b"mdat", b"audio"));

        let mut probe = FastStartProbe::default();
        for (index, chunk) in faststart.chunks(3).enumerate() {
            let decision = probe.push(chunk);
            let consumed = ((index + 1) * 3).min(faststart.len());
            if consumed < moov_end {
                assert_eq!(decision, FastStartDecision::Pending);
            } else {
                assert_eq!(decision, FastStartDecision::Progressive);
            }
        }

        let mut tail_moov = test_box(*b"ftyp", b"M4A ");
        tail_moov.extend(test_box(*b"mdat", b"audio"));
        tail_moov.extend(test_box(*b"moov", b"metadata"));
        assert_eq!(
            FastStartProbe::default().push(&tail_moov),
            FastStartDecision::ArtifactOnly
        );
    }

    #[test]
    fn falls_back_for_invalid_or_oversized_boxes() {
        let mut invalid = Vec::from(4_u32.to_be_bytes());
        invalid.extend_from_slice(b"free");
        assert_eq!(
            FastStartProbe::default().push(&invalid),
            FastStartDecision::ArtifactOnly
        );

        let mut oversized = test_box(*b"ftyp", b"M4A ");
        oversized.extend_from_slice(&1_u32.to_be_bytes());
        oversized.extend_from_slice(b"free");
        oversized.extend_from_slice(&(MAX_PROBE_BYTES + 1).to_be_bytes());
        assert_eq!(
            FastStartProbe::default().push(&oversized),
            FastStartDecision::ArtifactOnly
        );
    }

    fn test_box(box_type: [u8; 4], payload: &[u8]) -> Vec<u8> {
        let length = u32::try_from(payload.len() + 8).expect("test box length");
        let mut bytes = Vec::with_capacity(length as usize);
        bytes.extend_from_slice(&length.to_be_bytes());
        bytes.extend_from_slice(&box_type);
        bytes.extend_from_slice(payload);
        bytes
    }
}
