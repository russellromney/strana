use std::path::Path;

/// LadybugDB graph database backend.
pub struct Backend {
    db: lbug::Database,
}

impl Backend {
    /// Open or create a database at the given directory path.
    pub fn open(path: &Path) -> Result<Self, String> {
        let db = lbug::Database::new(path, lbug::SystemConfig::default())
            .map_err(|e| format!("Failed to open database: {e}"))?;
        Ok(Self { db })
    }

    /// Create a new connection to the database.
    pub fn connection(&self) -> Result<lbug::Connection<'_>, String> {
        lbug::Connection::new(&self.db)
            .map_err(|e| format!("Failed to create connection: {e}"))
    }
}
