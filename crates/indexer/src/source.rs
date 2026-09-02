use zakura_state::ZakuraDb;

use crate::{
    index::{Index, IndexState},
    pipeline::{HistoricalPipeline, PipelineConfig, PipelineError},
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
        let pipeline = HistoricalPipeline::new(index, &self.db, pipeline_config)?;
        let mut previous_tip = None;
        loop {
            let state = pipeline.sync()?;
            if previous_tip == Some(state.durable_tip()) {
                return Ok(state);
            }
            previous_tip = Some(state.durable_tip());
        }
    }
}
