use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

/// Result of rewriting a Cypher query for deterministic replay.
pub struct RewriteResult {
    /// The rewritten query text (unchanged if no non-deterministic functions found).
    pub query: String,
    /// Generated parameters to merge with user-supplied params.
    /// Keys are param names without `$` prefix (e.g., `_uuid_0`).
    pub generated_params: Vec<(String, serde_json::Value)>,
}

/// Rewrite non-deterministic function calls in a Cypher query.
///
/// Replaces:
///   gen_random_uuid()   -> $__graphd_uuid_N   (UUID v4 string value)
///   current_timestamp() -> $__graphd_now_N    (ISO 8601 timestamp string)
///   current_date()      -> $__graphd_date_N   (ISO 8601 date string)
///
/// Does NOT rewrite inside string literals ('...' or "...").
/// Case-insensitive. Handles optional whitespace between name and `()`.
pub fn rewrite_query(query: &str) -> RewriteResult {
    let bytes = query.as_bytes();
    let len = bytes.len();
    let mut output = Vec::with_capacity(len);
    let mut generated_params = Vec::new();
    let mut counters = [0u32; 3]; // [uuid, now, date]
    let mut i = 0;
    let mut in_single_quote = false;
    let mut in_double_quote = false;

    while i < len {
        if in_single_quote {
            output.push(bytes[i]);
            if bytes[i] == b'\'' {
                // Handle escaped single quote ''
                if i + 1 < len && bytes[i + 1] == b'\'' {
                    output.push(bytes[i + 1]);
                    i += 2;
                } else {
                    in_single_quote = false;
                    i += 1;
                }
            } else {
                i += 1;
            }
        } else if in_double_quote {
            output.push(bytes[i]);
            if bytes[i] == b'"' {
                in_double_quote = false;
            }
            i += 1;
        } else if bytes[i] == b'\'' {
            in_single_quote = true;
            output.push(bytes[i]);
            i += 1;
        } else if bytes[i] == b'"' {
            in_double_quote = true;
            output.push(bytes[i]);
            i += 1;
        } else if let Some((func_idx, consumed)) = try_match_func(bytes, i) {
            let (param_ref, param_name, param_value) = match func_idx {
                0 => {
                    let n = counters[0];
                    counters[0] += 1;
                    (format!("$__graphd_uuid_{n}"), format!("__graphd_uuid_{n}"), Uuid::new_v4().to_string())
                }
                1 => {
                    let n = counters[1];
                    counters[1] += 1;
                    (format!("$__graphd_now_{n}"), format!("__graphd_now_{n}"), generate_timestamp())
                }
                _ => {
                    let n = counters[2];
                    counters[2] += 1;
                    (format!("$__graphd_date_{n}"), format!("__graphd_date_{n}"), generate_date())
                }
            };
            output.extend_from_slice(param_ref.as_bytes());
            generated_params.push((param_name, serde_json::Value::String(param_value)));
            i += consumed;
        } else {
            output.push(bytes[i]);
            i += 1;
        }
    }

    let query_str = unsafe { String::from_utf8_unchecked(output) };
    let query_str = rewrite_remove(&query_str);

    RewriteResult {
        query: query_str,
        generated_params,
    }
}

/// Merge generated params into user-supplied params.
/// Returns `None` if both are empty/absent. Otherwise returns a merged JSON object.
pub fn merge_params(
    user_params: Option<&serde_json::Value>,
    generated: &[(String, serde_json::Value)],
) -> Option<serde_json::Value> {
    if generated.is_empty() {
        return user_params.cloned();
    }
    let mut obj = match user_params.and_then(|p| p.as_object()) {
        Some(o) => o.clone(),
        None => serde_json::Map::new(),
    };
    for (key, val) in generated {
        obj.insert(key.clone(), val.clone());
    }
    Some(serde_json::Value::Object(obj))
}

// ─── REMOVE rewrite ───

const REMOVE_KW: &[u8] = b"REMOVE";

/// Rewrite `REMOVE <expr>.<prop>` to `SET <expr>.<prop> = NULL`.
///
/// Handles single property removal only (not label removal or multi-item).
/// Respects string literals — does not rewrite inside '...' or "...".
/// Case-insensitive keyword matching with word boundary checks.
fn rewrite_remove(query: &str) -> String {
    let bytes = query.as_bytes();
    let len = bytes.len();
    let mut output = Vec::with_capacity(len);
    let mut i = 0;
    let mut in_single_quote = false;
    let mut in_double_quote = false;
    let mut changed = false;

    while i < len {
        // String literal tracking (same logic as rewrite_query)
        if in_single_quote {
            output.push(bytes[i]);
            if bytes[i] == b'\'' {
                if i + 1 < len && bytes[i + 1] == b'\'' {
                    output.push(bytes[i + 1]);
                    i += 2;
                } else {
                    in_single_quote = false;
                    i += 1;
                }
            } else {
                i += 1;
            }
            continue;
        }
        if in_double_quote {
            output.push(bytes[i]);
            if bytes[i] == b'"' {
                in_double_quote = false;
            }
            i += 1;
            continue;
        }
        if bytes[i] == b'\'' {
            in_single_quote = true;
            output.push(bytes[i]);
            i += 1;
            continue;
        }
        if bytes[i] == b'"' {
            in_double_quote = true;
            output.push(bytes[i]);
            i += 1;
            continue;
        }

        // Try to match REMOVE keyword
        if i + REMOVE_KW.len() <= len
            && bytes[i..i + REMOVE_KW.len()].eq_ignore_ascii_case(REMOVE_KW)
        {
            // Word boundary: not preceded by alphanumeric
            let before_ok = i == 0 || !bytes[i - 1].is_ascii_alphanumeric();
            // Word boundary: not followed by alphanumeric (after keyword)
            let after_pos = i + REMOVE_KW.len();
            let after_ok =
                after_pos >= len || !bytes[after_pos].is_ascii_alphanumeric();

            if before_ok && after_ok {
                // Scan ahead: skip whitespace, then look for <expr>.<prop> pattern.
                // A property expression is: identifier chars, then '.', then identifier chars.
                let mut j = after_pos;
                while j < len && bytes[j].is_ascii_whitespace() {
                    j += 1;
                }

                if let Some((prop_end, has_dot)) = scan_property_expr(bytes, j) {
                    if has_dot {
                        // It's a property removal: REMOVE n.prop → SET n.prop = NULL
                        output.extend_from_slice(b"SET ");
                        // Copy the property expression preserving original case
                        output.extend_from_slice(&bytes[j..prop_end]);
                        output.extend_from_slice(b" = NULL");
                        i = prop_end;
                        changed = true;
                        continue;
                    }
                }
            }
        }

        output.push(bytes[i]);
        i += 1;
    }

    if changed {
        unsafe { String::from_utf8_unchecked(output) }
    } else {
        query.to_string()
    }
}

/// Scan a property expression starting at `pos`.
/// Returns `(end_pos, has_dot)` where `has_dot` indicates it's a property access (n.prop)
/// vs a label expression (n:Label) which we don't rewrite.
///
/// Accepts identifiers with backtick quoting: `weird name`.prop
fn scan_property_expr(bytes: &[u8], pos: usize) -> Option<(usize, bool)> {
    let len = bytes.len();
    if pos >= len {
        return None;
    }

    // Scan first identifier (variable name): alphanumeric, underscore, or backtick-quoted
    let mut i = scan_identifier(bytes, pos)?;

    // Check what follows: '.' means property access, ':' means label, anything else = not a match
    if i >= len || bytes[i] != b'.' {
        return Some((i, false));
    }
    i += 1; // skip '.'

    // Scan property name
    let end = scan_identifier(bytes, i)?;
    Some((end, true))
}

/// Scan a single Cypher identifier at `pos`. Returns end position.
/// Handles plain identifiers (alphanumeric + underscore) and backtick-quoted identifiers.
fn scan_identifier(bytes: &[u8], pos: usize) -> Option<usize> {
    let len = bytes.len();
    if pos >= len {
        return None;
    }
    if bytes[pos] == b'`' {
        // Backtick-quoted identifier
        let mut i = pos + 1;
        while i < len && bytes[i] != b'`' {
            i += 1;
        }
        if i >= len {
            return None; // unclosed backtick
        }
        Some(i + 1) // past closing backtick
    } else if bytes[pos].is_ascii_alphabetic() || bytes[pos] == b'_' {
        let mut i = pos + 1;
        while i < len && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
            i += 1;
        }
        Some(i)
    } else {
        None
    }
}

// ─── Internals ───

/// Function name patterns to match (case-insensitive). Order matters:
/// check `current_timestamp` before `current_date` since it's longer.
const FUNC_NAMES: &[&[u8]] = &[
    b"gen_random_uuid",    // index 0
    b"current_timestamp",  // index 1
    b"current_date",       // index 2
];

/// Try to match a non-deterministic function call at byte position `pos`.
/// Returns `(func_index, bytes_consumed)` on match.
fn try_match_func(bytes: &[u8], pos: usize) -> Option<(usize, usize)> {
    for (idx, func_name) in FUNC_NAMES.iter().enumerate() {
        let name_len = func_name.len();
        if pos + name_len > bytes.len() {
            continue;
        }
        if !bytes[pos..pos + name_len].eq_ignore_ascii_case(func_name) {
            continue;
        }
        // Skip optional whitespace after function name
        let mut j = pos + name_len;
        while j < bytes.len() && bytes[j].is_ascii_whitespace() {
            j += 1;
        }
        // Expect '('
        if j >= bytes.len() || bytes[j] != b'(' {
            continue;
        }
        j += 1;
        // Skip whitespace inside parens
        while j < bytes.len() && bytes[j].is_ascii_whitespace() {
            j += 1;
        }
        // Expect ')'
        if j < bytes.len() && bytes[j] == b')' {
            return Some((idx, j + 1 - pos));
        }
    }
    None
}

/// Generate an ISO 8601 timestamp string from the current system time (UTC).
/// Format: `YYYY-MM-DDThh:mm:ss.uuuuuu`
fn generate_timestamp() -> String {
    let dur = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    let total_secs = dur.as_secs();
    let micros = dur.subsec_micros();
    let days = (total_secs / 86400) as i64;
    let day_secs = total_secs % 86400;
    let h = day_secs / 3600;
    let m = (day_secs % 3600) / 60;
    let s = day_secs % 60;
    let (y, mo, d) = days_to_ymd(days);
    format!("{y:04}-{mo:02}-{d:02}T{h:02}:{m:02}:{s:02}.{micros:06}")
}

/// Generate an ISO 8601 date string from the current system time (UTC).
/// Format: `YYYY-MM-DD`
fn generate_date() -> String {
    let dur = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    let days = (dur.as_secs() / 86400) as i64;
    let (y, m, d) = days_to_ymd(days);
    format!("{y:04}-{m:02}-{d:02}")
}

/// Convert days since Unix epoch (1970-01-01) to (year, month, day).
/// Howard Hinnant's civil_from_days algorithm.
fn days_to_ymd(days: i64) -> (i32, u32, u32) {
    let z = days + 719468;
    let era = (if z >= 0 { z } else { z - 146096 }) / 146097;
    let doe = (z - era * 146097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y as i32, m, d)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_no_rewrite_plain_query() {
        let r = rewrite_query("MATCH (n:Person) RETURN n.name");
        assert_eq!(r.query, "MATCH (n:Person) RETURN n.name");
        assert!(r.generated_params.is_empty());
    }

    #[test]
    fn test_no_rewrite_return_literal() {
        let r = rewrite_query("RETURN 42 AS x");
        assert_eq!(r.query, "RETURN 42 AS x");
        assert!(r.generated_params.is_empty());
    }

    #[test]
    fn test_rewrite_gen_random_uuid() {
        let r = rewrite_query("CREATE (:T {id: gen_random_uuid()})");
        assert_eq!(r.query, "CREATE (:T {id: $__graphd_uuid_0})");
        assert_eq!(r.generated_params.len(), 1);
        assert_eq!(r.generated_params[0].0, "__graphd_uuid_0");
        // Value should be a valid UUID
        let val = r.generated_params[0].1.as_str().unwrap();
        assert!(uuid::Uuid::parse_str(val).is_ok(), "Not a valid UUID: {val}");
    }

    #[test]
    fn test_rewrite_current_timestamp() {
        let r = rewrite_query("RETURN current_timestamp() AS ts");
        assert_eq!(r.query, "RETURN $__graphd_now_0 AS ts");
        assert_eq!(r.generated_params.len(), 1);
        assert_eq!(r.generated_params[0].0, "__graphd_now_0");
        let val = r.generated_params[0].1.as_str().unwrap();
        // Should match ISO 8601: YYYY-MM-DDThh:mm:ss.uuuuuu
        assert!(val.len() >= 26, "Timestamp too short: {val}");
        assert_eq!(&val[4..5], "-");
        assert_eq!(&val[7..8], "-");
        assert_eq!(&val[10..11], "T");
        assert_eq!(&val[13..14], ":");
        assert_eq!(&val[16..17], ":");
        assert_eq!(&val[19..20], ".");
    }

    #[test]
    fn test_rewrite_current_date() {
        let r = rewrite_query("RETURN current_date() AS d");
        assert_eq!(r.query, "RETURN $__graphd_date_0 AS d");
        assert_eq!(r.generated_params.len(), 1);
        assert_eq!(r.generated_params[0].0, "__graphd_date_0");
        let val = r.generated_params[0].1.as_str().unwrap();
        // Should match YYYY-MM-DD
        assert_eq!(val.len(), 10, "Date wrong length: {val}");
        assert_eq!(&val[4..5], "-");
        assert_eq!(&val[7..8], "-");
    }

    #[test]
    fn test_rewrite_multiple_same_function() {
        let r = rewrite_query("CREATE (:T {a: gen_random_uuid(), b: gen_random_uuid()})");
        assert_eq!(r.query, "CREATE (:T {a: $__graphd_uuid_0, b: $__graphd_uuid_1})");
        assert_eq!(r.generated_params.len(), 2);
        assert_eq!(r.generated_params[0].0, "__graphd_uuid_0");
        assert_eq!(r.generated_params[1].0, "__graphd_uuid_1");
        // Values should be distinct
        assert_ne!(r.generated_params[0].1, r.generated_params[1].1);
    }

    #[test]
    fn test_rewrite_mixed_functions() {
        let r = rewrite_query(
            "CREATE (:T {id: gen_random_uuid(), ts: current_timestamp(), d: current_date()})",
        );
        assert_eq!(
            r.query,
            "CREATE (:T {id: $__graphd_uuid_0, ts: $__graphd_now_0, d: $__graphd_date_0})"
        );
        assert_eq!(r.generated_params.len(), 3);
        assert_eq!(r.generated_params[0].0, "__graphd_uuid_0");
        assert_eq!(r.generated_params[1].0, "__graphd_now_0");
        assert_eq!(r.generated_params[2].0, "__graphd_date_0");
    }

    #[test]
    fn test_no_rewrite_in_single_quotes() {
        let r = rewrite_query("RETURN 'gen_random_uuid()' AS s");
        assert_eq!(r.query, "RETURN 'gen_random_uuid()' AS s");
        assert!(r.generated_params.is_empty());
    }

    #[test]
    fn test_no_rewrite_in_double_quotes() {
        let r = rewrite_query("RETURN \"gen_random_uuid()\" AS s");
        assert_eq!(r.query, "RETURN \"gen_random_uuid()\" AS s");
        assert!(r.generated_params.is_empty());
    }

    #[test]
    fn test_escaped_single_quotes() {
        // Cypher uses '' to escape a single quote inside a string
        let r = rewrite_query("RETURN 'it''s gen_random_uuid()' AS s");
        assert_eq!(r.query, "RETURN 'it''s gen_random_uuid()' AS s");
        assert!(r.generated_params.is_empty());
    }

    #[test]
    fn test_case_insensitive() {
        let r1 = rewrite_query("RETURN GEN_RANDOM_UUID()");
        assert_eq!(r1.query, "RETURN $__graphd_uuid_0");
        assert_eq!(r1.generated_params.len(), 1);

        let r2 = rewrite_query("RETURN Gen_Random_Uuid()");
        assert_eq!(r2.query, "RETURN $__graphd_uuid_0");
        assert_eq!(r2.generated_params.len(), 1);

        let r3 = rewrite_query("RETURN CURRENT_TIMESTAMP()");
        assert_eq!(r3.query, "RETURN $__graphd_now_0");

        let r4 = rewrite_query("RETURN CURRENT_DATE()");
        assert_eq!(r4.query, "RETURN $__graphd_date_0");
    }

    #[test]
    fn test_whitespace_between_name_and_parens() {
        let r = rewrite_query("RETURN gen_random_uuid ()");
        assert_eq!(r.query, "RETURN $__graphd_uuid_0");
        assert_eq!(r.generated_params.len(), 1);

        let r2 = rewrite_query("RETURN gen_random_uuid\t( )");
        assert_eq!(r2.query, "RETURN $__graphd_uuid_0");
        assert_eq!(r2.generated_params.len(), 1);
    }

    #[test]
    fn test_no_match_without_parens() {
        let r = rewrite_query("RETURN gen_random_uuid AS alias");
        assert_eq!(r.query, "RETURN gen_random_uuid AS alias");
        assert!(r.generated_params.is_empty());
    }

    #[test]
    fn test_no_match_with_args() {
        let r = rewrite_query("RETURN gen_random_uuid(42)");
        assert_eq!(r.query, "RETURN gen_random_uuid(42)");
        assert!(r.generated_params.is_empty());
    }

    #[test]
    fn test_preserves_surrounding_text() {
        let r = rewrite_query("SELECT 1; gen_random_uuid(); SELECT 2");
        assert_eq!(r.query, "SELECT 1; $__graphd_uuid_0; SELECT 2");
    }

    #[test]
    fn test_merge_params_empty() {
        let merged = merge_params(None, &[]);
        assert!(merged.is_none());
    }

    #[test]
    fn test_merge_params_only_user() {
        let user = serde_json::json!({"name": "Alice"});
        let merged = merge_params(Some(&user), &[]);
        assert_eq!(merged, Some(user));
    }

    #[test]
    fn test_merge_params_only_generated() {
        let gen = vec![("__graphd_uuid_0".to_string(), serde_json::json!("abc-123"))];
        let merged = merge_params(None, &gen).unwrap();
        assert_eq!(merged["__graphd_uuid_0"], "abc-123");
    }

    #[test]
    fn test_merge_params_both() {
        let user = serde_json::json!({"name": "Alice"});
        let gen = vec![("__graphd_uuid_0".to_string(), serde_json::json!("abc-123"))];
        let merged = merge_params(Some(&user), &gen).unwrap();
        assert_eq!(merged["name"], "Alice");
        assert_eq!(merged["__graphd_uuid_0"], "abc-123");
    }

    #[test]
    fn test_days_to_ymd_epoch() {
        assert_eq!(days_to_ymd(0), (1970, 1, 1));
    }

    #[test]
    fn test_days_to_ymd_known_date() {
        // 2024-01-15 is 19737 days after epoch
        assert_eq!(days_to_ymd(19737), (2024, 1, 15));
    }

    #[test]
    fn test_days_to_ymd_leap_year() {
        // 2024-02-29 is 19782 days after epoch
        assert_eq!(days_to_ymd(19782), (2024, 2, 29));
    }

    #[test]
    fn test_generated_uuid_is_valid_v4() {
        let r = rewrite_query("RETURN gen_random_uuid()");
        let val = r.generated_params[0].1.as_str().unwrap();
        let parsed = uuid::Uuid::parse_str(val).unwrap();
        assert_eq!(parsed.get_version_num(), 4);
    }

    // ─── REMOVE rewrite tests ───

    #[test]
    fn test_remove_property() {
        let r = rewrite_query("MATCH (n:Person) REMOVE n.age");
        assert_eq!(r.query, "MATCH (n:Person) SET n.age = NULL");
    }

    #[test]
    fn test_remove_property_case_insensitive() {
        let r = rewrite_query("MATCH (n) remove n.prop");
        assert_eq!(r.query, "MATCH (n) SET n.prop = NULL");

        let r2 = rewrite_query("MATCH (n) Remove n.prop");
        assert_eq!(r2.query, "MATCH (n) SET n.prop = NULL");
    }

    #[test]
    fn test_remove_property_extra_whitespace() {
        let r = rewrite_query("MATCH (n) REMOVE   n.prop");
        assert_eq!(r.query, "MATCH (n) SET n.prop = NULL");
    }

    #[test]
    fn test_remove_preserves_surrounding_query() {
        let r = rewrite_query("MATCH (n:Person) REMOVE n.age RETURN n");
        assert_eq!(r.query, "MATCH (n:Person) SET n.age = NULL RETURN n");
    }

    #[test]
    fn test_remove_underscore_identifiers() {
        let r = rewrite_query("MATCH (my_node) REMOVE my_node.some_prop");
        assert_eq!(r.query, "MATCH (my_node) SET my_node.some_prop = NULL");
    }

    #[test]
    fn test_remove_backtick_variable() {
        let r = rewrite_query("MATCH (`my node`) REMOVE `my node`.prop");
        assert_eq!(r.query, "MATCH (`my node`) SET `my node`.prop = NULL");
    }

    #[test]
    fn test_remove_backtick_property() {
        let r = rewrite_query("MATCH (n) REMOVE n.`weird prop`");
        assert_eq!(r.query, "MATCH (n) SET n.`weird prop` = NULL");
    }

    #[test]
    fn test_remove_not_in_string_literal() {
        let r = rewrite_query("RETURN 'REMOVE n.prop' AS s");
        assert_eq!(r.query, "RETURN 'REMOVE n.prop' AS s");
    }

    #[test]
    fn test_remove_not_in_double_quote_string() {
        let r = rewrite_query("RETURN \"REMOVE n.prop\" AS s");
        assert_eq!(r.query, "RETURN \"REMOVE n.prop\" AS s");
    }

    #[test]
    fn test_remove_label_not_rewritten() {
        // REMOVE n:Label is label removal — not rewritable to SET
        let r = rewrite_query("MATCH (n) REMOVE n:SomeLabel");
        // Should pass through unchanged (no dot = not a property removal)
        assert_eq!(r.query, "MATCH (n) REMOVE n:SomeLabel");
    }

    #[test]
    fn test_remove_word_boundary() {
        // "REMOVED" should not match (alphanumeric after keyword)
        let r = rewrite_query("RETURN REMOVED AS alias");
        assert_eq!(r.query, "RETURN REMOVED AS alias");
    }

    #[test]
    fn test_remove_combined_with_function_rewrite() {
        // Both rewrites should apply in the same query
        let r = rewrite_query("MATCH (n) REMOVE n.ts SET n.id = gen_random_uuid()");
        assert_eq!(
            r.query,
            "MATCH (n) SET n.ts = NULL SET n.id = $__graphd_uuid_0"
        );
        assert_eq!(r.generated_params.len(), 1);
    }
}
