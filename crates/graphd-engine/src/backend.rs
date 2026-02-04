use std::path::Path;

use crate::error::GraphdError;

/// LadybugDB graph database backend.
pub struct Backend {
    db: lbug::Database,
}

impl Backend {
    /// Open or create a database at the given directory path with default config.
    pub fn open(path: &Path) -> Result<Self, GraphdError> {
        Self::open_with_config(path, lbug::SystemConfig::default())
    }

    /// Open or create a database with memory and thread limits.
    ///
    /// This is the preferred constructor for multi-tenant environments.
    pub fn open_tenant(path: &Path, memory_mb: u64, max_threads: u64) -> Result<Self, GraphdError> {
        let config = lbug::SystemConfig::default()
            .buffer_pool_size(memory_mb * 1024 * 1024)
            .max_num_threads(max_threads);
        Self::open_with_config(path, config)
    }

    /// Open or create a database at the given directory path with custom config.
    pub fn open_with_config(path: &Path, config: lbug::SystemConfig) -> Result<Self, GraphdError> {
        let db = lbug::Database::new(path, config)
            .map_err(|e| GraphdError::DatabaseError(format!("Failed to open database: {e}")))?;
        Ok(Self { db })
    }

    /// Create a new connection to the database.
    pub fn connection(&self) -> Result<lbug::Connection<'_>, GraphdError> {
        lbug::Connection::new(&self.db)
            .map_err(|e| GraphdError::DatabaseError(format!("Connection failed: {e}")))
    }
}
