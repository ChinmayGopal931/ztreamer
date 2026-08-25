use crate::{
    index::RANGE_SIZE,
    parser::{CompactSaplingOutput, CompactShieldedAction, CompactTransaction},
};

const BLOCK_FORMAT_VERSION: u8 = 1;
const RANGE_FORMAT_VERSION: u8 = 1;
const MAX_RECORD_BYTES: usize = 2_000_000;
const RECORD_FIXED_BYTES: usize = 93;
const TRANSACTION_FIXED_BYTES: usize = 56;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TreeSizes {
    pub sapling: u32,
    pub orchard: u32,
    pub ironwood: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompactBlockRecord {
    pub height: u32,
    pub hash: [u8; 32],
    pub previous_hash: [u8; 32],
    pub time: u32,
    pub header: Vec<u8>,
    pub transactions: Vec<CompactTransaction>,
    pub end_tree_sizes: TreeSizes,
}

#[derive(Debug, thiserror::Error, Eq, PartialEq)]
pub enum CodecError {
    #[error("encoded value is truncated")]
    Truncated,
    #[error("unsupported {kind} format version {version}")]
    Version { kind: &'static str, version: u8 },
    #[error("encoded count, length, or offset is invalid")]
    Length,
    #[error("encoded value has trailing bytes")]
    TrailingBytes,
    #[error("range must contain exactly {RANGE_SIZE} contiguous aligned blocks")]
    InvalidRange,
    #[error("range record index is outside 0..{RANGE_SIZE}")]
    RangeIndex,
}

impl CompactBlockRecord {
    pub fn encode(&self) -> Result<Vec<u8>, CodecError> {
        let encoded_len = encoded_record_len(self.header.len(), &self.transactions)?;
        let mut bytes = Vec::with_capacity(encoded_len);
        bytes.push(BLOCK_FORMAT_VERSION);
        put_u32(&mut bytes, self.height);
        bytes.extend_from_slice(&self.hash);
        bytes.extend_from_slice(&self.previous_hash);
        put_u32(&mut bytes, self.time);
        put_u32(&mut bytes, self.end_tree_sizes.sapling);
        put_u32(&mut bytes, self.end_tree_sizes.orchard);
        put_u32(&mut bytes, self.end_tree_sizes.ironwood);
        put_len(&mut bytes, self.header.len())?;
        put_len(&mut bytes, self.transactions.len())?;
        bytes.extend_from_slice(&self.header);

        for transaction in &self.transactions {
            put_u64(&mut bytes, transaction.index);
            bytes.extend_from_slice(&transaction.txid);
            put_len(&mut bytes, transaction.sapling_spends.len())?;
            put_len(&mut bytes, transaction.sapling_outputs.len())?;
            put_len(&mut bytes, transaction.orchard_actions.len())?;
            put_len(&mut bytes, transaction.ironwood_actions.len())?;
            for nullifier in &transaction.sapling_spends {
                bytes.extend_from_slice(nullifier);
            }
            for output in &transaction.sapling_outputs {
                bytes.extend_from_slice(&output.cmu);
                bytes.extend_from_slice(&output.ephemeral_key);
                bytes.extend_from_slice(&output.ciphertext);
            }
            for action in transaction
                .orchard_actions
                .iter()
                .chain(&transaction.ironwood_actions)
            {
                bytes.extend_from_slice(&action.nullifier);
                bytes.extend_from_slice(&action.commitment);
                bytes.extend_from_slice(&action.ephemeral_key);
                bytes.extend_from_slice(&action.ciphertext);
            }
        }

        debug_assert_eq!(bytes.len(), encoded_len);
        Ok(bytes)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        if bytes.len() > MAX_RECORD_BYTES {
            return Err(CodecError::Length);
        }
        let mut reader = Reader::new(bytes);
        let version = reader.u8()?;
        if version != BLOCK_FORMAT_VERSION {
            return Err(CodecError::Version {
                kind: "block",
                version,
            });
        }
        let height = reader.u32()?;
        let hash = reader.array()?;
        let previous_hash = reader.array()?;
        let time = reader.u32()?;
        let end_tree_sizes = TreeSizes {
            sapling: reader.u32()?,
            orchard: reader.u32()?,
            ironwood: reader.u32()?,
        };
        let header_len = reader.len()?;
        let transaction_count = reader.len()?;
        let header = reader.take(header_len)?.to_vec();
        let mut transactions = Vec::with_capacity(reader.bounded_count(transaction_count, 56)?);

        for _ in 0..transaction_count {
            let index = reader.u64()?;
            let txid = reader.array()?;
            let spends = reader.len()?;
            let outputs = reader.len()?;
            let orchard = reader.len()?;
            let ironwood = reader.len()?;
            let required = spends
                .checked_mul(32)
                .and_then(|n| n.checked_add(outputs.checked_mul(116)?))
                .and_then(|n| n.checked_add(orchard.checked_mul(148)?))
                .and_then(|n| n.checked_add(ironwood.checked_mul(148)?))
                .ok_or(CodecError::Length)?;
            if required > reader.remaining() {
                return Err(CodecError::Truncated);
            }

            let sapling_spends = (0..spends)
                .map(|_| reader.array())
                .collect::<Result<_, _>>()?;
            let sapling_outputs = (0..outputs)
                .map(|_| {
                    Ok(CompactSaplingOutput {
                        cmu: reader.array()?,
                        ephemeral_key: reader.array()?,
                        ciphertext: reader.array()?,
                    })
                })
                .collect::<Result<_, CodecError>>()?;
            let mut read_actions = |count| {
                (0..count)
                    .map(|_| {
                        Ok(CompactShieldedAction {
                            nullifier: reader.array()?,
                            commitment: reader.array()?,
                            ephemeral_key: reader.array()?,
                            ciphertext: reader.array()?,
                        })
                    })
                    .collect::<Result<Vec<_>, CodecError>>()
            };
            let orchard_actions = read_actions(orchard)?;
            let ironwood_actions = read_actions(ironwood)?;
            transactions.push(CompactTransaction {
                index,
                txid,
                sapling_spends,
                sapling_outputs,
                orchard_actions,
                ironwood_actions,
            });
        }
        reader.finish()?;

        Ok(Self {
            height,
            hash,
            previous_hash,
            time,
            header,
            transactions,
            end_tree_sizes,
        })
    }
}

pub(crate) fn encoded_record_len(
    header_len: usize,
    transactions: &[CompactTransaction],
) -> Result<usize, CodecError> {
    let len = transactions.iter().try_fold(
        RECORD_FIXED_BYTES
            .checked_add(header_len)
            .ok_or(CodecError::Length)?,
        |len, transaction| {
            len.checked_add(TRANSACTION_FIXED_BYTES)?
                .checked_add(transaction.sapling_spends.len().checked_mul(32)?)?
                .checked_add(transaction.sapling_outputs.len().checked_mul(116)?)?
                .checked_add(transaction.orchard_actions.len().checked_mul(148)?)?
                .checked_add(transaction.ironwood_actions.len().checked_mul(148)?)
        },
    );
    len.filter(|len| *len <= MAX_RECORD_BYTES)
        .ok_or(CodecError::Length)
}

pub fn encode_range(records: &[CompactBlockRecord]) -> Result<Vec<u8>, CodecError> {
    if records.len() != RANGE_SIZE as usize
        || !records[0].height.is_multiple_of(RANGE_SIZE)
        || records.windows(2).any(|pair| {
            pair[0].height.checked_add(1) != Some(pair[1].height)
                || pair[1].previous_hash != pair[0].hash
        })
    {
        return Err(CodecError::InvalidRange);
    }

    let mut body = Vec::new();
    let mut offsets = Vec::with_capacity(records.len() + 1);
    for record in records {
        offsets.push(u32::try_from(body.len()).map_err(|_| CodecError::Length)?);
        let record = record.encode()?;
        put_len(&mut body, record.len())?;
        body.extend_from_slice(&record);
    }
    offsets.push(u32::try_from(body.len()).map_err(|_| CodecError::Length)?);

    let mut bytes = Vec::new();
    bytes.push(RANGE_FORMAT_VERSION);
    put_u32(&mut bytes, records[0].height);
    put_u32(
        &mut bytes,
        records.last().expect("range is non-empty").height,
    );
    bytes.extend_from_slice(&records[0].previous_hash);
    bytes.extend_from_slice(&records.last().expect("range is non-empty").hash);
    offsets
        .into_iter()
        .for_each(|offset| put_u32(&mut bytes, offset));
    bytes.extend_from_slice(&body);
    Ok(bytes)
}

pub fn decode_range_record(bytes: &[u8], index: usize) -> Result<CompactBlockRecord, CodecError> {
    if index >= RANGE_SIZE as usize {
        return Err(CodecError::RangeIndex);
    }
    let mut reader = Reader::new(bytes);
    let version = reader.u8()?;
    if version != RANGE_FORMAT_VERSION {
        return Err(CodecError::Version {
            kind: "range",
            version,
        });
    }
    let start = reader.u32()?;
    let end = reader.u32()?;
    let first_previous_hash = reader.array::<32>()?;
    let terminal_hash = reader.array::<32>()?;
    if !start.is_multiple_of(RANGE_SIZE) || start.checked_add(RANGE_SIZE - 1) != Some(end) {
        return Err(CodecError::InvalidRange);
    }

    let offsets = (0..=RANGE_SIZE)
        .map(|_| reader.u32().map(|offset| offset as usize))
        .collect::<Result<Vec<_>, _>>()?;
    let body = reader.take(reader.remaining())?;
    if offsets[0] != 0
        || offsets[RANGE_SIZE as usize] != body.len()
        || offsets.windows(2).any(|pair| pair[0] > pair[1])
    {
        return Err(CodecError::Length);
    }
    let envelope = body
        .get(offsets[index]..offsets[index + 1])
        .ok_or(CodecError::Length)?;
    let mut envelope = Reader::new(envelope);
    let record_len = envelope.len()?;
    let record = CompactBlockRecord::decode(envelope.take(record_len)?)?;
    envelope.finish()?;
    if start.checked_add(index as u32) != Some(record.height)
        || (index == 0 && record.previous_hash != first_previous_hash)
        || (index + 1 == RANGE_SIZE as usize && record.hash != terminal_hash)
    {
        return Err(CodecError::InvalidRange);
    }
    Ok(record)
}

fn put_u32(bytes: &mut Vec<u8>, value: u32) {
    bytes.extend_from_slice(&value.to_be_bytes());
}

fn put_u64(bytes: &mut Vec<u8>, value: u64) {
    bytes.extend_from_slice(&value.to_be_bytes());
}

fn put_len(bytes: &mut Vec<u8>, value: usize) -> Result<(), CodecError> {
    put_u32(bytes, value.try_into().map_err(|_| CodecError::Length)?);
    Ok(())
}

struct Reader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn remaining(&self) -> usize {
        self.bytes.len() - self.offset
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], CodecError> {
        let end = self.offset.checked_add(len).ok_or(CodecError::Length)?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or(CodecError::Truncated)?;
        self.offset = end;
        Ok(value)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], CodecError> {
        Ok(self.take(N)?.try_into().expect("length was checked"))
    }

    fn u8(&mut self) -> Result<u8, CodecError> {
        Ok(self.array::<1>()?[0])
    }

    fn u32(&mut self) -> Result<u32, CodecError> {
        Ok(u32::from_be_bytes(self.array()?))
    }

    fn u64(&mut self) -> Result<u64, CodecError> {
        Ok(u64::from_be_bytes(self.array()?))
    }

    fn len(&mut self) -> Result<usize, CodecError> {
        Ok(self.u32()? as usize)
    }

    fn bounded_count(&self, count: usize, minimum_size: usize) -> Result<usize, CodecError> {
        if count.checked_mul(minimum_size).ok_or(CodecError::Length)? > self.remaining() {
            return Err(CodecError::Truncated);
        }
        Ok(count)
    }

    fn finish(self) -> Result<(), CodecError> {
        if self.remaining() == 0 {
            Ok(())
        } else {
            Err(CodecError::TrailingBytes)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn block_and_range_round_trip() {
        let transaction = CompactTransaction {
            index: 2,
            txid: [3; 32],
            sapling_spends: vec![[4; 32]],
            sapling_outputs: vec![CompactSaplingOutput {
                cmu: [5; 32],
                ephemeral_key: [6; 32],
                ciphertext: [7; 52],
            }],
            orchard_actions: vec![action(8)],
            ironwood_actions: vec![action(9)],
        };
        let first = CompactBlockRecord {
            height: 0,
            hash: [1; 32],
            previous_hash: [0; 32],
            time: 10,
            header: vec![11; 140],
            transactions: vec![transaction],
            end_tree_sizes: TreeSizes {
                sapling: 1,
                orchard: 1,
                ironwood: 1,
            },
        };
        assert_eq!(
            CompactBlockRecord::decode(&first.encode().unwrap()).unwrap(),
            first
        );

        let mut records = vec![first];
        for height in 1..RANGE_SIZE {
            let previous_hash = records.last().unwrap().hash;
            records.push(CompactBlockRecord {
                height,
                hash: hash(height),
                previous_hash,
                time: height,
                header: Vec::new(),
                transactions: Vec::new(),
                end_tree_sizes: TreeSizes::default(),
            });
        }
        let encoded = encode_range(&records).unwrap();
        assert_eq!(decode_range_record(&encoded, 0).unwrap(), records[0]);
        assert_eq!(decode_range_record(&encoded, 537).unwrap(), records[537]);
        assert_eq!(decode_range_record(&encoded, 999).unwrap(), records[999]);
    }

    fn action(byte: u8) -> CompactShieldedAction {
        CompactShieldedAction {
            nullifier: [byte; 32],
            commitment: [byte; 32],
            ephemeral_key: [byte; 32],
            ciphertext: [byte; 52],
        }
    }

    fn hash(height: u32) -> [u8; 32] {
        let mut hash = [0; 32];
        hash[..4].copy_from_slice(&height.to_be_bytes());
        hash
    }
}
