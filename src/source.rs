use zakura_chain::{block::Height, parameters::Network};
use zakura_state::{
    Config, NonFinalizedState, ReadStateService, StateInitError, ZakuraDb, init_read_only,
};

use crate::{
    index::{Index, IndexState},
    pipeline::{PipelineConfig, PipelineError, sync_historical},
};

#[derive(Debug, thiserror::Error)]
pub enum ZakuraSyncError {
    #[error("failed to catch the Zakura secondary up with its primary: {0}")]
    CatchUp(String),
    #[error(transparent)]
    Pipeline(#[from] PipelineError),
}

/// One read-only Zakura secondary shared by historical ingestion and delegated RPC reads.
pub struct ZakuraSecondary {
    read_service: ReadStateService,
    db: ZakuraDb,
    _non_finalized_sender: tokio::sync::watch::Sender<NonFinalizedState>,
}

impl ZakuraSecondary {
    pub fn open(state_config: Config, network: &Network) -> Result<Self, StateInitError> {
        let (read_service, db, non_finalized_sender) = init_read_only(state_config, network)?;
        Ok(Self {
            read_service,
            db,
            _non_finalized_sender: non_finalized_sender,
        })
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
            self.db
                .try_catch_up_with_primary()
                .map_err(|error| ZakuraSyncError::CatchUp(error.to_string()))?;
            let state = sync_historical(
                index,
                |start, count, max_bytes| {
                    self.db
                        .read_compact_index_source_range(Height(start), count, max_bytes)
                },
                pipeline_config,
            )?;
            if previous_tip == Some(state.durable_tip()) {
                return Ok(state);
            }
            previous_tip = Some(state.durable_tip());
        }
    }
}
