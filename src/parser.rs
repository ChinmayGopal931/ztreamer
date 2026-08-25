use zakura_state::{RawIndexBlock, RawIndexTransaction};

const MAX_TRANSACTION_BYTES: usize = 2_000_000;
const MAX_VECTOR_ITEMS: usize = u16::MAX as usize;
const COMPACT_CIPHERTEXT_BYTES: usize = 52;

const OVERWINTER_GROUP_ID: u32 = 0x03c4_8270;
const SAPLING_GROUP_ID: u32 = 0x892f_2085;
const V5_GROUP_ID: u32 = 0x26a7_270a;
const V6_GROUP_ID: u32 = 0xd884_b698;

const SAPLING_V4_SPEND_BYTES: usize = 384;
const SAPLING_SPEND_PREFIX_BYTES: usize = 96;
const SAPLING_OUTPUT_PREFIX_BYTES: usize = 756;
const SAPLING_V4_OUTPUT_BYTES: usize = 948;
const ORCHARD_ACTION_BYTES: usize = 820;
const BCTV14_JOINSPLIT_BYTES: usize = 1_802;
const GROTH16_JOINSPLIT_BYTES: usize = 1_698;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompactSaplingOutput {
    pub cmu: [u8; 32],
    pub ephemeral_key: [u8; 32],
    pub ciphertext: [u8; COMPACT_CIPHERTEXT_BYTES],
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompactShieldedAction {
    pub nullifier: [u8; 32],
    pub commitment: [u8; 32],
    pub ephemeral_key: [u8; 32],
    pub ciphertext: [u8; COMPACT_CIPHERTEXT_BYTES],
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompactTransaction {
    pub index: u64,
    pub txid: [u8; 32],
    pub sapling_spends: Vec<[u8; 32]>,
    pub sapling_outputs: Vec<CompactSaplingOutput>,
    pub orchard_actions: Vec<CompactShieldedAction>,
    pub ironwood_actions: Vec<CompactShieldedAction>,
}

impl CompactTransaction {
    fn has_payload(&self) -> bool {
        !self.sapling_spends.is_empty()
            || !self.sapling_outputs.is_empty()
            || !self.orchard_actions.is_empty()
            || !self.ironwood_actions.is_empty()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedCompactBlock {
    pub height: u32,
    pub hash: [u8; 32],
    pub previous_hash: [u8; 32],
    pub time: u32,
    pub header: Vec<u8>,
    pub transactions: Vec<CompactTransaction>,
    pub sapling_additions: u32,
    pub orchard_additions: u32,
    pub ironwood_additions: u32,
}

#[derive(Debug, thiserror::Error)]
pub enum CompactParseError {
    #[error("block {height} has a truncated header")]
    TruncatedHeader { height: u32 },
    #[error("block {height} transaction {index}: {source}")]
    Transaction {
        height: u32,
        index: usize,
        #[source]
        source: TransactionParseError,
    },
    #[error("block {height} commitment count exceeds u32")]
    CommitmentCount { height: u32 },
}

#[derive(Debug, thiserror::Error)]
pub enum TransactionParseError {
    #[error("transaction is larger than {MAX_TRANSACTION_BYTES} bytes")]
    TooLarge,
    #[error("truncated transaction at byte {offset}")]
    Truncated { offset: usize },
    #[error("non-canonical CompactSize at byte {offset}")]
    NonCanonicalCompactSize { offset: usize },
    #[error("count or length at byte {offset} exceeds its bound")]
    LengthLimit { offset: usize },
    #[error("unsupported transaction header {header:#010x}")]
    UnsupportedHeader { header: u32 },
    #[error("wrong version group ID {actual:#010x}, expected {expected:#010x}")]
    VersionGroup { actual: u32, expected: u32 },
    #[error("trailing bytes at byte {offset}")]
    TrailingBytes { offset: usize },
}

pub fn parse_block(block: &RawIndexBlock) -> Result<PreparedCompactBlock, CompactParseError> {
    let height = block.height.0;
    let previous_hash = block
        .header
        .get(4..36)
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or(CompactParseError::TruncatedHeader { height })?;
    let time = u32::from_le_bytes(
        block
            .header
            .get(100..104)
            .and_then(|bytes| bytes.try_into().ok())
            .ok_or(CompactParseError::TruncatedHeader { height })?,
    );

    let mut transactions = Vec::new();
    let mut sapling_additions = 0usize;
    let mut orchard_additions = 0usize;
    let mut ironwood_additions = 0usize;

    for (index, transaction) in block.transactions.iter().enumerate() {
        let compact = parse_transaction(transaction, index as u64).map_err(|source| {
            CompactParseError::Transaction {
                height,
                index,
                source,
            }
        })?;
        sapling_additions = sapling_additions
            .checked_add(compact.sapling_outputs.len())
            .ok_or(CompactParseError::CommitmentCount { height })?;
        orchard_additions = orchard_additions
            .checked_add(compact.orchard_actions.len())
            .ok_or(CompactParseError::CommitmentCount { height })?;
        ironwood_additions = ironwood_additions
            .checked_add(compact.ironwood_actions.len())
            .ok_or(CompactParseError::CommitmentCount { height })?;
        if compact.has_payload() {
            transactions.push(compact);
        }
    }

    Ok(PreparedCompactBlock {
        height,
        hash: block.hash.0,
        previous_hash,
        time,
        header: block.header.clone(),
        transactions,
        sapling_additions: sapling_additions
            .try_into()
            .map_err(|_| CompactParseError::CommitmentCount { height })?,
        orchard_additions: orchard_additions
            .try_into()
            .map_err(|_| CompactParseError::CommitmentCount { height })?,
        ironwood_additions: ironwood_additions
            .try_into()
            .map_err(|_| CompactParseError::CommitmentCount { height })?,
    })
}

fn parse_transaction(
    transaction: &RawIndexTransaction,
    index: u64,
) -> Result<CompactTransaction, TransactionParseError> {
    if transaction.bytes.len() > MAX_TRANSACTION_BYTES {
        return Err(TransactionParseError::TooLarge);
    }

    let mut reader = Reader::new(&transaction.bytes);
    let header = reader.u32()?;
    let version = header & 0x7fff_ffff;
    let overwintered = header >> 31 != 0;
    let mut compact = CompactTransaction {
        index,
        txid: transaction.txid.0,
        sapling_spends: Vec::new(),
        sapling_outputs: Vec::new(),
        orchard_actions: Vec::new(),
        ironwood_actions: Vec::new(),
    };

    match (version, overwintered) {
        (1, false) => {
            reader.transparent_bundle()?;
            reader.skip(4)?;
        }
        (2, false) => {
            reader.transparent_bundle()?;
            reader.skip(4)?;
            reader.joinsplits(BCTV14_JOINSPLIT_BYTES)?;
        }
        (3, true) => {
            reader.version_group(OVERWINTER_GROUP_ID)?;
            reader.transparent_bundle()?;
            reader.skip(8)?;
            reader.joinsplits(BCTV14_JOINSPLIT_BYTES)?;
        }
        (4, true) => {
            reader.version_group(SAPLING_GROUP_ID)?;
            reader.transparent_bundle()?;
            reader.skip(16)?;
            reader.sapling_v4(&mut compact)?;
        }
        (5, true) => {
            reader.version_group(V5_GROUP_ID)?;
            reader.skip(12)?;
            reader.transparent_bundle()?;
            reader.sapling_v5(&mut compact)?;
            compact.orchard_actions = reader.actions()?;
        }
        (6, true) => {
            reader.version_group(V6_GROUP_ID)?;
            reader.skip(12)?;
            reader.transparent_bundle()?;
            reader.sapling_v5(&mut compact)?;
            compact.orchard_actions = reader.actions()?;
            compact.ironwood_actions = reader.actions()?;
        }
        _ => return Err(TransactionParseError::UnsupportedHeader { header }),
    }

    reader.finish()?;
    Ok(compact)
}

struct Reader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], TransactionParseError> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or(TransactionParseError::LengthLimit {
                offset: self.offset,
            })?;
        let bytes = self
            .bytes
            .get(self.offset..end)
            .ok_or(TransactionParseError::Truncated {
                offset: self.offset,
            })?;
        self.offset = end;
        Ok(bytes)
    }

    fn skip(&mut self, len: usize) -> Result<(), TransactionParseError> {
        self.take(len).map(|_| ())
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], TransactionParseError> {
        Ok(self.take(N)?.try_into().expect("length was checked"))
    }

    fn u32(&mut self) -> Result<u32, TransactionParseError> {
        Ok(u32::from_le_bytes(self.array()?))
    }

    fn compact_size(&mut self, max: usize) -> Result<usize, TransactionParseError> {
        let start = self.offset;
        let first = self.array::<1>()?[0];
        let value = match first {
            0..=252 => u64::from(first),
            253 => {
                let value = u16::from_le_bytes(self.array()?);
                if value < 253 {
                    return Err(TransactionParseError::NonCanonicalCompactSize { offset: start });
                }
                u64::from(value)
            }
            254 => {
                let value = self.u32()?;
                if value <= u16::MAX.into() {
                    return Err(TransactionParseError::NonCanonicalCompactSize { offset: start });
                }
                u64::from(value)
            }
            255 => {
                let value = u64::from_le_bytes(self.array()?);
                if value <= u32::MAX.into() {
                    return Err(TransactionParseError::NonCanonicalCompactSize { offset: start });
                }
                value
            }
        };
        let value = usize::try_from(value)
            .ok()
            .filter(|value| *value <= max)
            .ok_or(TransactionParseError::LengthLimit { offset: start })?;
        Ok(value)
    }

    fn count(&mut self) -> Result<usize, TransactionParseError> {
        self.compact_size(MAX_VECTOR_ITEMS)
    }

    fn skip_items(&mut self, count: usize, size: usize) -> Result<(), TransactionParseError> {
        let bytes = count
            .checked_mul(size)
            .ok_or(TransactionParseError::LengthLimit {
                offset: self.offset,
            })?;
        self.skip(bytes)
    }

    fn ensure_items(&self, count: usize, size: usize) -> Result<(), TransactionParseError> {
        let bytes = count
            .checked_mul(size)
            .ok_or(TransactionParseError::LengthLimit {
                offset: self.offset,
            })?;
        if bytes > self.bytes.len() - self.offset {
            return Err(TransactionParseError::Truncated {
                offset: self.offset,
            });
        }
        Ok(())
    }

    fn version_group(&mut self, expected: u32) -> Result<(), TransactionParseError> {
        let actual = self.u32()?;
        if actual != expected {
            return Err(TransactionParseError::VersionGroup { actual, expected });
        }
        Ok(())
    }

    fn transparent_bundle(&mut self) -> Result<(), TransactionParseError> {
        let inputs = self.count()?;
        for _ in 0..inputs {
            self.skip(36)?;
            let script = self.compact_size(MAX_TRANSACTION_BYTES)?;
            self.skip(script)?;
            self.skip(4)?;
        }
        let outputs = self.count()?;
        for _ in 0..outputs {
            self.skip(8)?;
            let script = self.compact_size(MAX_TRANSACTION_BYTES)?;
            self.skip(script)?;
        }
        Ok(())
    }

    fn joinsplits(&mut self, item_size: usize) -> Result<(), TransactionParseError> {
        let count = self.count()?;
        self.skip_items(count, item_size)?;
        if count > 0 {
            self.skip(96)?;
        }
        Ok(())
    }

    fn sapling_v4(
        &mut self,
        compact: &mut CompactTransaction,
    ) -> Result<(), TransactionParseError> {
        let spends = self.count()?;
        self.ensure_items(spends, SAPLING_V4_SPEND_BYTES)?;
        compact.sapling_spends.reserve(spends);
        for _ in 0..spends {
            self.skip(64)?;
            compact.sapling_spends.push(self.array()?);
            self.skip(SAPLING_V4_SPEND_BYTES - 96)?;
        }

        let outputs = self.count()?;
        self.ensure_items(outputs, SAPLING_V4_OUTPUT_BYTES)?;
        compact.sapling_outputs.reserve(outputs);
        for _ in 0..outputs {
            compact.sapling_outputs.push(self.sapling_output(true)?);
        }

        self.joinsplits(GROTH16_JOINSPLIT_BYTES)?;
        if spends > 0 || outputs > 0 {
            self.skip(64)?;
        }
        Ok(())
    }

    fn sapling_v5(
        &mut self,
        compact: &mut CompactTransaction,
    ) -> Result<(), TransactionParseError> {
        let spends = self.count()?;
        self.ensure_items(spends, SAPLING_SPEND_PREFIX_BYTES)?;
        compact.sapling_spends.reserve(spends);
        for _ in 0..spends {
            self.skip(32)?;
            compact.sapling_spends.push(self.array()?);
            self.skip(SAPLING_SPEND_PREFIX_BYTES - 64)?;
        }

        let outputs = self.count()?;
        self.ensure_items(outputs, SAPLING_OUTPUT_PREFIX_BYTES)?;
        compact.sapling_outputs.reserve(outputs);
        for _ in 0..outputs {
            compact.sapling_outputs.push(self.sapling_output(false)?);
        }

        if spends > 0 || outputs > 0 {
            self.skip(8)?;
            if spends > 0 {
                self.skip(32)?;
            }
            self.skip_items(spends, 192 + 64)?;
            self.skip_items(outputs, 192)?;
            self.skip(64)?;
        }
        Ok(())
    }

    fn sapling_output(
        &mut self,
        includes_proof: bool,
    ) -> Result<CompactSaplingOutput, TransactionParseError> {
        self.skip(32)?;
        let cmu = self.array()?;
        let ephemeral_key = self.array()?;
        let ciphertext = self.array()?;
        self.skip(580 - COMPACT_CIPHERTEXT_BYTES + 80)?;
        if includes_proof {
            self.skip(SAPLING_V4_OUTPUT_BYTES - SAPLING_OUTPUT_PREFIX_BYTES)?;
        }
        Ok(CompactSaplingOutput {
            cmu,
            ephemeral_key,
            ciphertext,
        })
    }

    fn actions(&mut self) -> Result<Vec<CompactShieldedAction>, TransactionParseError> {
        let count = self.count()?;
        self.ensure_items(count, ORCHARD_ACTION_BYTES)?;
        let mut actions = Vec::with_capacity(count);
        for _ in 0..count {
            self.skip(32)?;
            let nullifier = self.array()?;
            self.skip(32)?;
            let commitment = self.array()?;
            let ephemeral_key = self.array()?;
            let ciphertext = self.array()?;
            self.skip(580 - COMPACT_CIPHERTEXT_BYTES + 80)?;
            actions.push(CompactShieldedAction {
                nullifier,
                commitment,
                ephemeral_key,
                ciphertext,
            });
        }

        if count > 0 {
            self.skip(1 + 8 + 32)?;
            let proof = self.compact_size(MAX_TRANSACTION_BYTES)?;
            self.skip(proof)?;
            self.skip_items(count, 64)?;
            self.skip(64)?;
        }
        Ok(actions)
    }

    fn finish(self) -> Result<(), TransactionParseError> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(TransactionParseError::TrailingBytes {
                offset: self.offset,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use zakura_chain::{
        block::{Block, Height},
        parameters::NetworkUpgrade,
        serialization::{ZcashDeserialize as _, ZcashSerialize as _},
        transaction::{LockTime, Transaction},
    };

    const SAPLING_BLOCK: &str =
        include_str!("../../zakura/crates/zakura-test/src/vectors/block-main-0-949-496.txt");
    const ORCHARD_BLOCK: &str =
        include_str!("../../zakura/crates/zakura-test/src/vectors/block-test-1-842-421.txt");
    const SAPLING_SPEND_BLOCK: &str =
        include_str!("../../zakura/crates/zakura-test/src/vectors/block-main-1-687-106.txt");

    #[test]
    fn selective_parser_matches_full_zakura_parse() {
        let mut saw_sapling = false;
        let mut saw_sapling_spend = false;
        let mut saw_orchard = false;

        for encoded_block in [SAPLING_BLOCK, SAPLING_SPEND_BLOCK, ORCHARD_BLOCK] {
            let bytes = hex::decode(encoded_block.trim()).unwrap();
            let block = Block::zcash_deserialize(bytes.as_slice()).unwrap();
            for (index, transaction) in block.transactions.iter().enumerate() {
                let bytes = transaction.zcash_serialize_to_vec().unwrap();
                let raw = RawIndexTransaction {
                    txid: transaction.hash(),
                    bytes,
                };
                let actual = parse_transaction(&raw, index as u64).unwrap();
                let expected = reference(transaction, index as u64);
                saw_sapling |= !expected.sapling_outputs.is_empty();
                saw_sapling_spend |= !expected.sapling_spends.is_empty();
                saw_orchard |= !expected.orchard_actions.is_empty();
                assert_eq!(actual, expected);
            }
        }

        assert!(saw_sapling && saw_sapling_spend && saw_orchard);
    }

    #[test]
    fn v6_keeps_orchard_and_ironwood_separate() {
        let bytes = hex::decode(ORCHARD_BLOCK.trim()).unwrap();
        let block = Block::zcash_deserialize(bytes.as_slice()).unwrap();
        let shielded = block
            .transactions
            .iter()
            .find_map(|transaction| transaction.orchard_shielded_data().cloned())
            .unwrap();
        let transaction = Transaction::V6 {
            network_upgrade: NetworkUpgrade::Nu6_3,
            lock_time: LockTime::unlocked(),
            expiry_height: Height(1),
            inputs: Vec::new(),
            outputs: Vec::new(),
            sapling_shielded_data: None,
            orchard_shielded_data: Some(shielded.clone()),
            ironwood_shielded_data: Some(shielded),
        };
        let bytes = transaction.zcash_serialize_to_vec().unwrap();
        let parsed = Transaction::zcash_deserialize(bytes.as_slice()).unwrap();
        let raw = RawIndexTransaction {
            txid: parsed.hash(),
            bytes,
        };

        assert_eq!(parse_transaction(&raw, 0).unwrap(), reference(&parsed, 0));
    }

    #[test]
    fn malformed_lengths_are_bounded() {
        let mut bytes = vec![1, 0, 0, 0, 0xff];
        bytes.extend_from_slice(&u64::MAX.to_le_bytes());
        let raw = RawIndexTransaction {
            txid: zakura_chain::transaction::Hash([0; 32]),
            bytes,
        };
        assert!(matches!(
            parse_transaction(&raw, 0),
            Err(TransactionParseError::LengthLimit { .. })
        ));
    }

    fn reference(transaction: &Transaction, index: u64) -> CompactTransaction {
        let sapling_spends = transaction
            .sapling_spends_per_anchor()
            .map(|spend| spend.nullifier.into())
            .collect();
        let sapling_outputs = transaction
            .sapling_outputs()
            .map(|output| {
                let encrypted: [u8; 580] = output.enc_ciphertext.into();
                CompactSaplingOutput {
                    cmu: output.cm_u.to_bytes(),
                    ephemeral_key: (&output.ephemeral_key).into(),
                    ciphertext: encrypted[..COMPACT_CIPHERTEXT_BYTES].try_into().unwrap(),
                }
            })
            .collect();
        let action = |action: &zakura_chain::orchard::Action| {
            let encrypted: [u8; 580] = action.enc_ciphertext.into();
            CompactShieldedAction {
                nullifier: action.nullifier.into(),
                commitment: action.cm_x.into(),
                ephemeral_key: (&action.ephemeral_key).into(),
                ciphertext: encrypted[..COMPACT_CIPHERTEXT_BYTES].try_into().unwrap(),
            }
        };

        CompactTransaction {
            index,
            txid: transaction.hash().0,
            sapling_spends,
            sapling_outputs,
            orchard_actions: transaction.orchard_actions().map(action).collect(),
            ironwood_actions: transaction.ironwood_actions().map(action).collect(),
        }
    }
}
