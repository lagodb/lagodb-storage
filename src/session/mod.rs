//! Per-connection session state: handle table and connection context.
//!
//! These types are shared between [`crate::connection`] (connection pipeline) and [`crate::service`]
//! (command dispatch), so they live at the crate root rather than inside either consumer.

mod context;
mod handle_table;

pub(crate) use context::AttachedStorageContext;
pub(crate) use context::StorageContext;
pub(crate) use handle_table::{
    HandleTable, OpenHandleSlot, ReadHandleGuard, ReservedOpen,
};

#[cfg(test)]
mod test_support;
