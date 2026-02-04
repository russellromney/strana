/// Typed errors for the graphd-engine API, aligned with Neo4j's error classification.
///
/// Each variant maps directly to a `Neo.{Classification}.{Category}.{Title}` code.
/// Drivers use these codes for retry logic, error categorization, and user display.

/// Neo4j-compatible error classification.
///
/// Variants follow Neo4j's three-tier hierarchy:
/// - **ClientError** (4xx): Client sent a bad request — not retryable.
/// - **DatabaseError** (5xx): Server failed — not retryable by default.
/// - **TransientError**: Temporary condition — retryable.
#[derive(Debug, Clone)]
pub enum GraphdError {
    /// `Neo.ClientError.Security.Forbidden` — operation blocked by policy.
    /// Sandbox violations, disallowed CALL options, blocked statements.
    Forbidden(String),

    /// `Neo.ClientError.Statement.SyntaxError` — query parse or execution error.
    /// Covers both Cypher syntax errors and runtime query failures from the engine.
    SyntaxError(String),

    /// `Neo.ClientError.Statement.TypeError` — invalid parameter types.
    /// Unsupported number format, objects passed as params, non-object params value.
    TypeError(String),

    /// `Neo.TransientError.General.DatabaseUnavailable` — engine temporarily unavailable.
    /// Semaphore/mutex closed, connection pool exhausted, shutdown in progress.
    DatabaseUnavailable(String),

    /// `Neo.DatabaseError.General.UnknownError` — internal server error.
    /// Task panics, connection failures, database open failures, unexpected state.
    DatabaseError(String),
}

impl std::fmt::Display for GraphdError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Forbidden(msg)
            | Self::SyntaxError(msg)
            | Self::TypeError(msg)
            | Self::DatabaseUnavailable(msg)
            | Self::DatabaseError(msg) => write!(f, "{msg}"),
        }
    }
}

impl std::error::Error for GraphdError {}

impl GraphdError {
    /// Full Neo4j error code string (e.g. `Neo.ClientError.Security.Forbidden`).
    pub fn neo4j_code(&self) -> &'static str {
        match self {
            Self::Forbidden(_) => "Neo.ClientError.Security.Forbidden",
            Self::SyntaxError(_) => "Neo.ClientError.Statement.SyntaxError",
            Self::TypeError(_) => "Neo.ClientError.Statement.TypeError",
            Self::DatabaseUnavailable(_) => "Neo.TransientError.General.DatabaseUnavailable",
            Self::DatabaseError(_) => "Neo.DatabaseError.General.UnknownError",
        }
    }

    /// Whether this is a client error (bad request, not retryable).
    pub fn is_client_error(&self) -> bool {
        matches!(
            self,
            Self::Forbidden(_) | Self::SyntaxError(_) | Self::TypeError(_)
        )
    }

    /// Whether the client should retry this request.
    pub fn is_retryable(&self) -> bool {
        matches!(self, Self::DatabaseUnavailable(_))
    }

    /// HTTP status code for this error.
    pub fn http_status(&self) -> u16 {
        match self {
            Self::Forbidden(_) => 403,
            Self::SyntaxError(_) | Self::TypeError(_) => 400,
            Self::DatabaseUnavailable(_) => 503,
            Self::DatabaseError(_) => 500,
        }
    }

    /// The inner error message.
    pub fn message(&self) -> &str {
        match self {
            Self::Forbidden(msg)
            | Self::SyntaxError(msg)
            | Self::TypeError(msg)
            | Self::DatabaseUnavailable(msg)
            | Self::DatabaseError(msg) => msg,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_preserves_message() {
        let err = GraphdError::SyntaxError("Binder exception: Table Person does not exist".into());
        assert_eq!(
            err.to_string(),
            "Binder exception: Table Person does not exist"
        );
    }

    #[test]
    fn neo4j_codes_match_classification() {
        assert_eq!(
            GraphdError::Forbidden("x".into()).neo4j_code(),
            "Neo.ClientError.Security.Forbidden"
        );
        assert_eq!(
            GraphdError::SyntaxError("x".into()).neo4j_code(),
            "Neo.ClientError.Statement.SyntaxError"
        );
        assert_eq!(
            GraphdError::TypeError("x".into()).neo4j_code(),
            "Neo.ClientError.Statement.TypeError"
        );
        assert_eq!(
            GraphdError::DatabaseUnavailable("x".into()).neo4j_code(),
            "Neo.TransientError.General.DatabaseUnavailable"
        );
        assert_eq!(
            GraphdError::DatabaseError("x".into()).neo4j_code(),
            "Neo.DatabaseError.General.UnknownError"
        );
    }

    #[test]
    fn client_errors() {
        assert!(GraphdError::Forbidden("x".into()).is_client_error());
        assert!(GraphdError::SyntaxError("x".into()).is_client_error());
        assert!(GraphdError::TypeError("x".into()).is_client_error());
        assert!(!GraphdError::DatabaseUnavailable("x".into()).is_client_error());
        assert!(!GraphdError::DatabaseError("x".into()).is_client_error());
    }

    #[test]
    fn retryable_only_transient() {
        assert!(!GraphdError::Forbidden("x".into()).is_retryable());
        assert!(!GraphdError::SyntaxError("x".into()).is_retryable());
        assert!(!GraphdError::TypeError("x".into()).is_retryable());
        assert!(GraphdError::DatabaseUnavailable("x".into()).is_retryable());
        assert!(!GraphdError::DatabaseError("x".into()).is_retryable());
    }

    #[test]
    fn http_status_codes() {
        assert_eq!(GraphdError::Forbidden("x".into()).http_status(), 403);
        assert_eq!(GraphdError::SyntaxError("x".into()).http_status(), 400);
        assert_eq!(GraphdError::TypeError("x".into()).http_status(), 400);
        assert_eq!(GraphdError::DatabaseUnavailable("x".into()).http_status(), 503);
        assert_eq!(GraphdError::DatabaseError("x".into()).http_status(), 500);
    }

    #[test]
    fn message_returns_inner_string() {
        assert_eq!(GraphdError::Forbidden("blocked".into()).message(), "blocked");
        assert_eq!(GraphdError::SyntaxError("bad query".into()).message(), "bad query");
        assert_eq!(GraphdError::DatabaseError("oops".into()).message(), "oops");
    }
}
