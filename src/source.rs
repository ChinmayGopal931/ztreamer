use zakura_chain::{block::Height, parameters::Network};
use zakura_state::{Config, StateInitError, init_read_only};

use crate::{
    index::{Index, IndexState},
    pipeline::{PipelineConfig, PipelineError, sync_historical},
};

#[derive(Debug, thiserror::Error)]
pub enum ZakuraSyncError {
    #[error(transparent)]
    Open(#[from] StateInitError),
    #[error("failed to catch the Zakura secondary up with its primary: {0}")]
    CatchUp(String),
    #[error(transparent)]
    Pipeline(#[from] PipelineError),
}

/// Opens Zakura's finalized database as a secondary, catches it up, and ingests its durable prefix.
pub fn sync_zakura_secondary(
    index: &Index,
    state_config: Config,
    network: &Network,
    pipeline_config: PipelineConfig,
) -> Result<IndexState, ZakuraSyncError> {
    let (_read_service, db, _non_finalized_sender) = init_read_only(state_config, network)?;
    let mut previous_tip = None;
    loop {
        db.try_catch_up_with_primary()
            .map_err(|error| ZakuraSyncError::CatchUp(error.to_string()))?;
        let state = sync_historical(
            index,
            |start, count, max_bytes| {
                db.read_compact_index_source_range(Height(start), count, max_bytes)
            },
            pipeline_config,
        )?;
        if previous_tip == Some(state.durable_tip()) {
            return Ok(state);
        }
        previous_tip = Some(state.durable_tip());
    }
}
