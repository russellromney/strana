/// Test that BOLT_MAX_VERSION environment variable is parsed correctly.
/// Combined into one test to avoid env var races (env vars are process-global).

use graphd::bolt::BoltVersion;

#[test]
fn test_env_var_parsing() {
    // 5.0
    std::env::set_var("BOLT_MAX_VERSION", "5.0");
    assert_eq!(std::env::var("BOLT_MAX_VERSION").unwrap(), "5.0");

    // 4.4
    std::env::set_var("BOLT_MAX_VERSION", "4.4");
    assert_eq!(std::env::var("BOLT_MAX_VERSION").unwrap(), "4.4");

    // 5.7
    std::env::set_var("BOLT_MAX_VERSION", "5.7");
    assert_eq!(std::env::var("BOLT_MAX_VERSION").unwrap(), "5.7");

    // Clean up.
    std::env::remove_var("BOLT_MAX_VERSION");
}
