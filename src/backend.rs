use std::path::Path;

/// LadybugDB graph database backend.
pub struct Backend {
    db: lbug::Database,
    query_timeout_ms: u64,
}

impl Backend {
    /// Open or create a database at the given directory path.
    pub fn open(path: &Path) -> Result<Self, String> {
        let db = lbug::Database::new(path, lbug::SystemConfig::default())
            .map_err(|e| format!("Failed to open database: {e}"))?;
        Ok(Self { db, query_timeout_ms: 0 })
    }

    /// Set query timeout for all new connections (0 = disabled).
    pub fn set_query_timeout_ms(&mut self, timeout_ms: u64) {
        self.query_timeout_ms = timeout_ms;
    }

    /// Create a new connection to the database.
    pub fn connection(&self) -> Result<lbug::Connection<'_>, String> {
        let conn = lbug::Connection::new(&self.db)
            .map_err(|e| format!("Failed to create connection: {e}"))?;
        if self.query_timeout_ms > 0 {
            conn.set_query_timeout(self.query_timeout_ms);
        }
        Ok(conn)
    }
}
