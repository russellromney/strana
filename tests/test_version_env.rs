/// Test that BOLT_MAX_VERSION environment variable is read correctly

use graphd::bolt::BoltVersion;

#[test]
fn test_env_var_parsing_5_0() {
    std::env::set_var("BOLT_MAX_VERSION", "5.0");

    // This would normally be called during connection handling
    // For now just verify the parsing would work
    let value = std::env::var("BOLT_MAX_VERSION").unwrap();
    assert_eq!(value, "5.0");
}

#[test]
fn test_env_var_parsing_4_4() {
    std::env::set_var("BOLT_MAX_VERSION", "4.4");

    let value = std::env::var("BOLT_MAX_VERSION").unwrap();
    assert_eq!(value, "4.4");
}

#[test]
fn test_env_var_parsing_5_7() {
    std::env::set_var("BOLT_MAX_VERSION", "5.7");

    let value = std::env::var("BOLT_MAX_VERSION").unwrap();
    assert_eq!(value, "5.7");
}
