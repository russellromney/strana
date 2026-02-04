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
    "INSTALL", "UNINSTALL", "IMPORT", "ATTACH", "DETACH",
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

    // ─── New mutation keywords (INSTALL, UNINSTALL, IMPORT, ATTACH, DETACH) ───

    #[test]
    fn test_is_mutation_install() {
        assert!(is_mutation("INSTALL extension_name"));
        assert!(is_mutation("install myext"));
        assert!(is_mutation("Install SomeExtension"));
    }

    #[test]
    fn test_is_mutation_uninstall() {
        assert!(is_mutation("UNINSTALL extension_name"));
        assert!(is_mutation("uninstall myext"));
    }

    #[test]
    fn test_is_mutation_import() {
        assert!(is_mutation("IMPORT DATABASE '/path/to/db'"));
        assert!(is_mutation("import database '/tmp/backup'"));
    }

    #[test]
    fn test_is_mutation_attach() {
        assert!(is_mutation("ATTACH DATABASE '/path/to/other.db'"));
        assert!(is_mutation("attach database '/tmp/other'"));
    }

    #[test]
    fn test_is_mutation_detach() {
        assert!(is_mutation("DETACH DATABASE other_db"));
        assert!(is_mutation("detach database mydb"));
    }

    #[test]
    fn test_uninstall_not_caught_by_install_boundary() {
        // UNINSTALL must be caught by its own keyword, not by INSTALL
        // (word boundary: 'N' before INSTALL is alphanumeric → INSTALL alone won't match)
        assert!(is_mutation("UNINSTALL ext"));
    }

    #[test]
    fn test_not_mutation_new_keywords_substring() {
        // Suffixed forms should not match
        assert!(!is_mutation("RETURN 'INSTALLED'"));
        assert!(!is_mutation("RETURN 'IMPORTING'"));
        assert!(!is_mutation("RETURN 'ATTACHED'"));
        assert!(!is_mutation("RETURN 'DETACHED'"));
        assert!(!is_mutation("RETURN 'DETACHING'"));
    }

    #[test]
    fn test_not_mutation_new_keywords_prefixed() {
        // Prefixed forms should not match (alpha before keyword)
        assert!(!is_mutation("RETURN 'REIMPORT'"));
        assert!(!is_mutation("RETURN 'REATTACH'"));
    }

    // ─── Comprehensive replica safety: every ladybug mutation type ───

    /// This test documents every mutation type that ladybug supports
    /// and verifies is_mutation catches all of them. If ladybug adds
    /// new mutation types, add them here.
    #[test]
    fn test_replica_safety_all_mutation_types() {
        // ── Data modification clauses ──
        assert!(is_mutation("CREATE (:Person {name: 'Alice'})"));
        assert!(is_mutation("CREATE NODE TABLE Person(name STRING, PRIMARY KEY(name))"));
        assert!(is_mutation("CREATE REL TABLE KNOWS(FROM Person TO Person)"));
        assert!(is_mutation("MATCH (a:Person), (b:Person) CREATE (a)-[:KNOWS]->(b)"));
        assert!(is_mutation("MERGE (n:Person {name: 'Bob'})"));
        assert!(is_mutation("MATCH (n:Person) SET n.age = 30"));
        assert!(is_mutation("MATCH (n:Person) DELETE n"));
        assert!(is_mutation("MATCH (n:Person) DETACH DELETE n"));

        // ── DDL ──
        assert!(is_mutation("DROP TABLE Person"));
        assert!(is_mutation("DROP TABLE IF EXISTS Person"));
        assert!(is_mutation("ALTER TABLE Person ADD age INT64"));
        assert!(is_mutation("ALTER TABLE Person DROP col"));
        assert!(is_mutation("ALTER TABLE Person RENAME TO People"));
        assert!(is_mutation("CREATE SEQUENCE my_seq"));
        assert!(is_mutation("DROP SEQUENCE my_seq"));
        assert!(is_mutation("CREATE TYPE my_type AS STRUCT(x INT64)"));
        assert!(is_mutation("CREATE MACRO my_func(x) AS x + 1"));
        assert!(is_mutation("DROP MACRO my_func"));
        assert!(is_mutation("CREATE GRAPH my_graph"));
        assert!(is_mutation("DROP GRAPH my_graph"));

        // ── Data import/export ──
        assert!(is_mutation("COPY Person FROM 'people.csv'"));
        assert!(is_mutation("COPY Person FROM 'people.parquet'"));

        // ── Extension management ──
        assert!(is_mutation("INSTALL httpfs"));
        assert!(is_mutation("UNINSTALL httpfs"));

        // ── Database management ──
        assert!(is_mutation("IMPORT DATABASE '/path/to/export'"));
        assert!(is_mutation("ATTACH DATABASE '/other/db' AS other"));
        assert!(is_mutation("DETACH DATABASE other"));

        // ── Read-only operations (must NOT trigger) ──
        assert!(!is_mutation("MATCH (n:Person) RETURN n"));
        assert!(!is_mutation("MATCH (n:Person) RETURN n.name ORDER BY n.name"));
        assert!(!is_mutation("MATCH (n:Person) RETURN count(n)"));
        assert!(!is_mutation("RETURN 1"));
        assert!(!is_mutation("RETURN 'hello world'"));
        assert!(!is_mutation("UNWIND [1, 2, 3] AS x RETURN x"));
        assert!(!is_mutation("CALL current_setting('threads')"));
        assert!(!is_mutation("EXPLAIN MATCH (n) RETURN n"));
        assert!(!is_mutation("EXPORT DATABASE '/path/to/export'"));
        assert!(!is_mutation("LOAD EXTENSION httpfs"));
        assert!(!is_mutation("USE DATABASE other"));
        assert!(!is_mutation("USE GRAPH my_graph"));
    }
}
