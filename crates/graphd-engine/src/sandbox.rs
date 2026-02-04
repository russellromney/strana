//! Statement-level sandboxing for multi-tenant graph databases.
//!
//! Uses keyword-based query text analysis to block dangerous operations.
//! Prevents filesystem I/O, cross-tenant escape, and extension loading.

use std::collections::HashSet;

use crate::error::GraphdError;

/// Configuration for statement-level sandboxing.
pub struct SandboxConfig {
    allowed_call_options: HashSet<String>,
}

impl SandboxConfig {
    /// Create a sandbox config suitable for multi-tenant environments.
    ///
    /// Blocks: COPY FROM/TO, EXPORT/IMPORT/ATTACH/DETACH DATABASE, LOAD EXTENSION.
    /// Filters: CALL statements by option name allowlist.
    /// Allows: Everything else (MATCH, CREATE, DROP, ALTER, EXPLAIN, etc.).
    pub fn multi_tenant() -> Self {
        let allowed_call_options: HashSet<String> = [
            "timeout",
            "threads",
            "warning_limit",
            "progress_bar",
            "var_length_extend_max_depth",
            "recursive_pattern_semantic",
            "recursive_pattern_factor",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();

        Self {
            allowed_call_options,
        }
    }

    /// Check whether a query is allowed by the sandbox.
    ///
    /// Analyzes query keywords to block dangerous operations.
    /// Returns `Ok(())` if allowed, `Err(message)` if blocked.
    pub fn check_query(&self, _conn: &lbug::Connection<'_>, query: &str) -> Result<(), GraphdError> {
        self.check_query_text(query)
    }

    /// Check query text without needing a connection (for testing).
    pub fn check_query_text(&self, query: &str) -> Result<(), GraphdError> {
        let normalized = query.trim();
        if normalized.is_empty() {
            return Ok(());
        }

        let upper = normalized.to_uppercase();
        let tokens = first_tokens(&upper, 3);

        // Block all COPY statements (COPY FROM / COPY TO)
        if tokens.first().map_or(false, |t| t == "COPY") {
            return Err(GraphdError::Forbidden("COPY is blocked in multi-tenant mode".into()));
        }

        // Block EXPORT DATABASE / IMPORT DATABASE
        if tokens.first().map_or(false, |t| t == "EXPORT" || t == "IMPORT") {
            return Err(GraphdError::Forbidden(format!(
                "{} is blocked in multi-tenant mode",
                tokens[0]
            )));
        }

        // Block ATTACH / DETACH
        if tokens.first().map_or(false, |t| t == "ATTACH" || t == "DETACH") {
            return Err(GraphdError::Forbidden(format!(
                "{} is blocked in multi-tenant mode",
                tokens[0]
            )));
        }

        // Block USE DATABASE (but allow USE GRAPH)
        if tokens.first().map_or(false, |t| t == "USE") {
            if tokens.get(1).map_or(false, |t| t != "GRAPH") {
                return Err(GraphdError::Forbidden("USE DATABASE is blocked in multi-tenant mode".into()));
            }
        }

        // Block LOAD EXTENSION / INSTALL
        if tokens.first().map_or(false, |t| t == "LOAD" || t == "INSTALL") {
            return Err(GraphdError::Forbidden(format!(
                "{} is blocked in multi-tenant mode",
                tokens[0]
            )));
        }

        // Filter CALL statements by option name
        if tokens.first().map_or(false, |t| t == "CALL") {
            return self.check_call_statement(query);
        }

        Ok(())
    }

    /// Check a CALL statement by parsing the option name from the query.
    fn check_call_statement(&self, query: &str) -> Result<(), GraphdError> {
        let trimmed = query.trim();
        let after_call = if let Some(rest) = strip_prefix_ci(trimmed, "CALL") {
            rest.trim_start()
        } else {
            return Err(GraphdError::Forbidden("Expected CALL statement".into()));
        };

        let option_name: String = after_call
            .chars()
            .take_while(|c| c.is_alphanumeric() || *c == '_')
            .collect();

        if option_name.is_empty() {
            return Err(GraphdError::Forbidden(
                "Could not parse option name from CALL statement".into(),
            ));
        }

        let option_lower = option_name.to_lowercase();
        if self.allowed_call_options.contains(&option_lower) {
            Ok(())
        } else {
            Err(GraphdError::Forbidden(format!(
                "CALL option '{}' is not allowed in multi-tenant mode",
                option_name
            )))
        }
    }
}

/// Extract the first N whitespace-separated tokens from a string.
fn first_tokens(s: &str, n: usize) -> Vec<String> {
    s.split_whitespace().take(n).map(String::from).collect()
}

/// Case-insensitive prefix strip.
fn strip_prefix_ci<'a>(s: &'a str, prefix: &str) -> Option<&'a str> {
    if s.len() >= prefix.len() && s[..prefix.len()].eq_ignore_ascii_case(prefix) {
        Some(&s[prefix.len()..])
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sandbox() -> SandboxConfig {
        SandboxConfig::multi_tenant()
    }

    // === Allowed queries ===

    #[test]
    fn allows_match() {
        assert!(sandbox().check_query_text("MATCH (n) RETURN n").is_ok());
    }

    #[test]
    fn allows_create_node_table() {
        assert!(sandbox()
            .check_query_text("CREATE NODE TABLE Person(name STRING, PRIMARY KEY(name))")
            .is_ok());
    }

    #[test]
    fn allows_create_rel_table() {
        assert!(sandbox()
            .check_query_text("CREATE REL TABLE Knows(FROM Person TO Person)")
            .is_ok());
    }

    #[test]
    fn allows_create_node() {
        assert!(sandbox()
            .check_query_text("CREATE (:Person {name: 'Alice'})")
            .is_ok());
    }

    #[test]
    fn allows_drop() {
        assert!(sandbox().check_query_text("DROP TABLE Person").is_ok());
    }

    #[test]
    fn allows_alter() {
        assert!(sandbox()
            .check_query_text("ALTER TABLE Person ADD age INT64")
            .is_ok());
    }

    #[test]
    fn allows_explain() {
        assert!(sandbox()
            .check_query_text("EXPLAIN MATCH (n) RETURN n")
            .is_ok());
    }

    #[test]
    fn allows_return() {
        assert!(sandbox().check_query_text("RETURN 1 + 2").is_ok());
    }

    #[test]
    fn allows_use_graph() {
        assert!(sandbox().check_query_text("USE GRAPH my_graph").is_ok());
    }

    // === Blocked queries ===

    #[test]
    fn blocks_copy_from() {
        assert!(sandbox()
            .check_query_text("COPY Person FROM '/etc/passwd'")
            .is_err());
    }

    #[test]
    fn blocks_copy_to() {
        assert!(sandbox()
            .check_query_text("COPY (MATCH (n) RETURN n) TO '/tmp/data.csv'")
            .is_err());
    }

    #[test]
    fn blocks_export_database() {
        assert!(sandbox()
            .check_query_text("EXPORT DATABASE '/tmp/export'")
            .is_err());
    }

    #[test]
    fn blocks_import_database() {
        assert!(sandbox()
            .check_query_text("IMPORT DATABASE '/tmp/import'")
            .is_err());
    }

    #[test]
    fn blocks_attach() {
        assert!(sandbox()
            .check_query_text("ATTACH '/tmp/other.db' AS other")
            .is_err());
    }

    #[test]
    fn blocks_detach() {
        assert!(sandbox().check_query_text("DETACH other").is_err());
    }

    #[test]
    fn blocks_use_database() {
        assert!(sandbox().check_query_text("USE other_db").is_err());
    }

    #[test]
    fn blocks_load_extension() {
        assert!(sandbox()
            .check_query_text("LOAD EXTENSION httpfs")
            .is_err());
    }

    #[test]
    fn blocks_install() {
        assert!(sandbox().check_query_text("INSTALL httpfs").is_err());
    }

    // === CALL filtering ===

    #[test]
    fn call_option_allowed() {
        let sb = sandbox();
        assert!(sb.check_query_text("CALL timeout = 5000").is_ok());
        assert!(sb.check_query_text("CALL threads = 2").is_ok());
        assert!(sb.check_query_text("CALL warning_limit = 100").is_ok());
    }

    #[test]
    fn call_option_blocked() {
        let sb = sandbox();
        assert!(sb.check_query_text("CALL home_directory = '/tmp'").is_err());
        assert!(sb.check_query_text("CALL file_search_path = '/etc'").is_err());
        assert!(sb.check_query_text("CALL auto_checkpoint = false").is_err());
        assert!(sb.check_query_text("CALL enable_multi_writes = true").is_err());
    }

    #[test]
    fn call_case_insensitive() {
        let sb = sandbox();
        assert!(sb.check_query_text("CALL Timeout = 5000").is_ok());
        assert!(sb.check_query_text("CALL THREADS = 2").is_ok());
    }

    // === Edge cases ===

    #[test]
    fn case_insensitive_blocks() {
        assert!(sandbox().check_query_text("copy Person FROM '/etc/passwd'").is_err());
        assert!(sandbox().check_query_text("EXPORT database '/tmp'").is_err());
        assert!(sandbox().check_query_text("attach '/tmp/db'").is_err());
    }

    #[test]
    fn empty_query_allowed() {
        assert!(sandbox().check_query_text("").is_ok());
        assert!(sandbox().check_query_text("   ").is_ok());
    }
}
