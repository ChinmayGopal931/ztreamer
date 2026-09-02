use zakura_state::ZakuraDb;

use crate::{
    index::{Index, IndexState},
    pipeline::{PipelineConfig, PipelineError, sync_historical},
};

#[derive(Debug, thiserror::Error)]
pub enum ZakuraSyncError {
    #[error(transparent)]
    Pipeline(#[from] PipelineError),
}

/// In-process access to Zakura's finalized database.
pub struct ZakuraSource {
    db: ZakuraDb,
}

impl ZakuraSource {
    pub fn new(db: ZakuraDb) -> Self {
        Self { db }
    }

    // Starts the historical sync process, populating the index
    pub fn sync(
        &self,
        index: &Index,
        pipeline_config: PipelineConfig,
    ) -> Result<IndexState, ZakuraSyncError> {
        let mut previous_tip = None;
        loop {
            let state = sync_historical(index, &self.db, pipeline_config)?;
            if previous_tip == Some(state.durable_tip()) {
                return Ok(state);
            }
            previous_tip = Some(state.durable_tip());
        }
    }
}
