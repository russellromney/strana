use std::path::Path;

/// Kuzu graph database backend.
pub struct KuzuBackend {
    db: kuzu::Database,
}

impl KuzuBackend {
    /// Open or create a Kuzu database at the given directory path.
    pub fn open(path: &Path) -> Result<Self, String> {
        let db = kuzu::Database::new(path, kuzu::SystemConfig::default())
            .map_err(|e| format!("Failed to open Kuzu database: {e}"))?;
        Ok(Self { db })
    }

    /// Create a new connection to the database.
    pub fn connection(&self) -> Result<kuzu::Connection<'_>, String> {
        kuzu::Connection::new(&self.db)
            .map_err(|e| format!("Failed to create connection: {e}"))
    }
}
