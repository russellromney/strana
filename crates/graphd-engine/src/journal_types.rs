//! Minimal journal types shared between graphd-engine and the graphd binary.
//!
//! The full journal writer/reader lives in the graphd binary. This module
//! provides only the types needed by the engine for mutation detection and
//! optional write-ahead logging.

/// A pending journal entry (query + params) to be written.
pub struct PendingEntry {
    pub query: String,
    pub params: Option<serde_json::Value>,
}

/// Commands sent to the journal writer thread.
pub enum JournalCommand {
    Write(PendingEntry),
    Flush(std::sync::mpsc::SyncSender<()>),
    Shutdown,
}

/// Channel sender for journal commands.
pub type JournalSender = std::sync::mpsc::Sender<JournalCommand>;

// ─── Mutation detection ───

const MUTATION_KEYWORDS: &[&str] = &[
    "CREATE", "MERGE", "DELETE", "DROP", "ALTER", "COPY", "SET",
];

/// Returns true if the query likely contains a mutation keyword.
/// Over-journaling reads is harmless; missing mutations would be catastrophic.
pub fn is_mutation(query: &str) -> bool {
    let upper = query.to_ascii_uppercase();
    let bytes = upper.as_bytes();
    for keyword in MUTATION_KEYWORDS {
        let kw_bytes = keyword.as_bytes();
        let kw_len = kw_bytes.len();
        let mut i = 0;
        while i + kw_len <= bytes.len() {
            if &bytes[i..i + kw_len] == kw_bytes {
                let before_ok = i == 0 || !bytes[i - 1].is_ascii_alphanumeric();
                let after_ok =
                    i + kw_len >= bytes.len() || !bytes[i + kw_len].is_ascii_alphanumeric();
                if before_ok && after_ok {
                    return true;
                }
            }
            i += 1;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mutation_keywords() {
        assert!(is_mutation("CREATE (:Person {name: 'Alice'})"));
        assert!(is_mutation("MERGE (n:Person)"));
        assert!(is_mutation("DELETE n"));
        assert!(is_mutation("DROP TABLE foo"));
        assert!(is_mutation("ALTER TABLE foo"));
        assert!(is_mutation("SET n.name = 'Bob'"));
        assert!(is_mutation("COPY FROM 'file.csv'"));
    }

    #[test]
    fn test_non_mutations() {
        assert!(!is_mutation("MATCH (n) RETURN n"));
        assert!(!is_mutation("RETURN 1 AS n"));
        assert!(!is_mutation("EXPLAIN MATCH (n) RETURN n"));
    }

    #[test]
    fn test_word_boundary() {
        // "CREATED" should not match "CREATE"
        assert!(!is_mutation("MATCH (n:CREATED) RETURN n"));
        // But "CREATE " should
        assert!(is_mutation("CREATE NODE TABLE Foo(id INT64, PRIMARY KEY(id))"));
    }
}
