use zakura_chain::block::Height;
use zakura_state::{ReadStateService, ZakuraDb};
use zakurad::node::NodeClient;

use crate::{
    index::{Index, IndexState},
    parser::RawIndexBlock,
    pipeline::{PipelineConfig, PipelineError, SourceRange, sync_historical},
};

#[derive(Debug, thiserror::Error)]
pub enum ZakuraSyncError {
    #[error(transparent)]
    Pipeline(#[from] PipelineError),
}

/// In-process access to Zakura's finalized database and state reads.
pub struct ZakuraSource {
    read_service: ReadStateService,
    db: ZakuraDb,
}

impl ZakuraSource {
    pub fn new(client: &NodeClient) -> Self {
        Self {
            read_service: client.read_state(),
            db: client.database(),
        }
    }

    pub fn read_service(&self) -> ReadStateService {
        self.read_service.clone()
    }

    pub fn sync(
        &self,
        index: &Index,
        pipeline_config: PipelineConfig,
    ) -> Result<IndexState, ZakuraSyncError> {
        let mut previous_tip = None;
        loop {
            let state = sync_historical(
                index,
                |start, count, max_bytes| self.read_range(start, count, max_bytes),
                pipeline_config,
            )?;
            if previous_tip == Some(state.durable_tip()) {
                return Ok(state);
            }
            previous_tip = Some(state.durable_tip());
        }
    }

    fn read_range(
        &self,
        start: u32,
        count: u32,
        max_bytes: usize,
    ) -> Result<SourceRange, PipelineError> {
        let source_tip = self.db.tip();
        let retained_body_floor = self.db.prune_height().unwrap_or(Height::MIN);
        let mut blocks = Vec::new();
        let mut response_bytes = 0usize;

        for offset in 0..count {
            let Some(height) = start.checked_add(offset).map(Height) else {
                break;
            };
            if source_tip.is_none_or(|(tip, _)| height > tip) {
                break;
            }
            let hash = self.db.hash(height).ok_or(PipelineError::SourceGap {
                expected: height.0,
                actual: None,
            })?;
            let bytes = self
                .db
                .raw_block_bytes(height.into())
                .ok_or(PipelineError::MissingBody { height: height.0 })?;
            let txids = self
                .db
                .transaction_hashes_for_block(height.into())
                .ok_or(PipelineError::MissingBody { height: height.0 })?
                .to_vec();
            let block_bytes = bytes
                .len()
                .checked_add(
                    txids
                        .len()
                        .checked_mul(32)
                        .ok_or(PipelineError::SourceSize { height: height.0 })?,
                )
                .ok_or(PipelineError::SourceSize { height: height.0 })?;
            let next_response_bytes = response_bytes
                .checked_add(block_bytes)
                .ok_or(PipelineError::SourceSize { height: height.0 })?;
            if next_response_bytes > max_bytes {
                if blocks.is_empty() {
                    return Err(PipelineError::BlockExceedsByteLimit {
                        height: height.0,
                        required: block_bytes,
                        limit: max_bytes,
                    });
                }
                break;
            }
            response_bytes = next_response_bytes;
            blocks.push(RawIndexBlock {
                height,
                hash,
                bytes,
                txids,
            });
        }

        Ok(SourceRange {
            blocks,
            retained_body_floor,
            source_tip,
        })
    }
}
