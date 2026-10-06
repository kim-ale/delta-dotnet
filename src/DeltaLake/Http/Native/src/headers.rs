use std::fmt;

use http::{HeaderMap, HeaderName, HeaderValue};
use serde::de::{self, MapAccess, Visitor};
use serde::Deserializer;

use crate::protocol::MAX_BYTES;

const MAX_ENTRIES: usize = 32;
const MAX_NAME: usize = 128;
const MAX_VALUE: usize = 8192;

struct HeaderVisitor;

impl<'de> Visitor<'de> for HeaderVisitor {
    type Value = HeaderMap;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a bounded string dictionary")
    }

    fn visit_map<Map: MapAccess<'de>>(self, mut map: Map) -> Result<HeaderMap, Map::Error> {
        let mut headers = HeaderMap::new();
        while let Some((name, value)) = map.next_entry::<String, String>()? {
            if headers.len() >= MAX_ENTRIES || name.len() > MAX_NAME || value.len() > MAX_VALUE {
                return Err(de::Error::custom("invalid headers"));
            }
            let name = HeaderName::from_bytes(name.as_bytes())
                .map_err(|_| de::Error::custom("invalid headers"))?;
            if forbidden(&name) || headers.contains_key(&name) {
                return Err(de::Error::custom("invalid headers"));
            }
            let mut value =
                HeaderValue::from_str(&value).map_err(|_| de::Error::custom("invalid headers"))?;
            value.set_sensitive(true);
            headers.insert(name, value);
        }
        Ok(headers)
    }
}

fn forbidden(name: &HeaderName) -> bool {
    matches!(
        name.as_str(),
        "authorization"
            | "host"
            | "cookie"
            | "content-length"
            | "transfer-encoding"
            | "connection"
            | "upgrade"
            | "x-ms-date"
            | "x-ms-version"
    ) || name.as_str().starts_with("proxy-")
}

pub(crate) fn parse(bytes: &[u8]) -> Result<HeaderMap, ()> {
    if bytes.is_empty() || bytes.len() > MAX_BYTES {
        return Err(());
    }
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    let headers = deserializer
        .deserialize_map(HeaderVisitor)
        .map_err(|_| ())?;
    deserializer.end().map_err(|_| ())?;
    Ok(headers)
}

pub(crate) fn validate_for_request(
    headers: &HeaderMap,
    request_headers: &HeaderMap,
) -> Result<(), ()> {
    let shared_key = request_headers
        .get_all("authorization")
        .iter()
        .any(|value| {
            value
                .as_bytes()
                .split(|byte| byte.is_ascii_whitespace())
                .next()
                .is_some_and(|scheme| scheme.eq_ignore_ascii_case(b"SharedKey"))
        });
    if shared_key
        && headers.keys().any(|name| {
            name.as_str().starts_with("x-ms")
                || matches!(
                    name.as_str(),
                    "content-encoding"
                        | "content-language"
                        | "content-length"
                        | "content-md5"
                        | "content-type"
                        | "date"
                        | "if-modified-since"
                        | "if-match"
                        | "if-none-match"
                        | "if-unmodified-since"
                        | "range"
                )
        })
    {
        return Err(());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shared_key_signed_headers_are_rejected() {
        for authorization in [
            "SharedKey test:signature",
            "sharedkey test:signature",
            "sHaReDkEy\ttest:signature",
        ] {
            let mut request_headers = HeaderMap::new();
            request_headers.insert(
                "authorization",
                HeaderValue::from_str(authorization).unwrap(),
            );
            let original = request_headers.clone();
            for name in [
                "X-MS-Client-Request-ID",
                "x-ms-meta-custom",
                "x-ms-custom",
                "x-mscustom",
                "Content-Encoding",
                "Content-Language",
                "Content-MD5",
                "Content-Type",
                "Date",
                "If-Modified-Since",
                "If-Match",
                "If-None-Match",
                "If-Unmodified-Since",
                "Range",
            ] {
                let headers = parse(format!(r#"{{"{name}":"secret"}}"#).as_bytes()).unwrap();
                assert!(
                    validate_for_request(&headers, &request_headers).is_err(),
                    "{name}"
                );
            }
            for headers in [
                parse(br#"{"x-custom":"secret"}"#).unwrap(),
                HeaderMap::new(),
            ] {
                assert!(validate_for_request(&headers, &request_headers).is_ok());
            }
            assert_eq!(request_headers, original);
        }
        let mut request_headers = HeaderMap::new();
        request_headers.append("authorization", HeaderValue::from_static("Bearer native"));
        request_headers.append(
            "authorization",
            HeaderValue::from_static("SharedKey test:signature"),
        );
        let headers = parse(br#"{"x-ms-client-request-id":"secret"}"#).unwrap();
        assert!(validate_for_request(&headers, &request_headers).is_err());
    }

    #[test]
    fn shared_key_guard_preserves_bearer_and_unsigned_headers() {
        let headers = parse(
            br#"{"x-ms-client-request-id":"request","x-ms-proxy-host":"fabric","Content-Type":"application/json"}"#,
        )
        .unwrap();
        let mut request_headers = HeaderMap::new();
        assert!(validate_for_request(&headers, &request_headers).is_ok());
        for authorization in [
            "Bearer native",
            "Bearer SharedKey",
            "SharedKeyOther test:signature",
        ] {
            request_headers.insert(
                "authorization",
                HeaderValue::from_str(authorization).unwrap(),
            );
            assert!(validate_for_request(&headers, &request_headers).is_ok());
        }
    }

    #[test]
    fn generic_string_headers_are_sensitive() {
        let headers = parse(br#"{"X-Custom":"secret","x-empty":""}"#).unwrap();
        assert!(headers["x-custom"].is_sensitive());
        assert_eq!(headers["x-custom"], "secret");
        assert!(!format!("{headers:?}").contains("secret"));
        assert!(headers["x-empty"].is_sensitive());
        assert!(parse(b"{}").unwrap().is_empty());
    }

    #[test]
    fn controlled_headers_are_rejected_case_insensitively() {
        for name in [
            "Authorization",
            "Host",
            "Cookie",
            "Content-Length",
            "Transfer-Encoding",
            "Connection",
            "Upgrade",
            "Proxy-Authorization",
            "Proxy-Anything",
            "X-MS-Date",
            "x-ms-version",
        ] {
            assert!(
                parse(format!(r#"{{"{name}":"secret"}}"#).as_bytes()).is_err(),
                "{name}"
            );
        }
    }

    #[test]
    fn malformed_dictionary_and_duplicates_are_rejected() {
        for bytes in [
            &b"[]"[..],
            b"null",
            b"1",
            b"{\"x\":1}",
            b"{\"x\":null}",
            b"{\"x\":{}}",
            b"{\"x\":\"a\",\"x\":\"b\"}",
            b"{\"X\":\"a\",\"x\":\"b\"}",
            b"{\"bad name\":\"secret\"}",
            b"{\"x\":\"secret\\r\\ninjected:yes\"}",
            b"{} trailing",
            b"\xff",
            b"",
        ] {
            assert!(parse(bytes).is_err(), "must reject malformed dictionary");
        }
    }

    #[test]
    fn header_limits_accept_boundary_and_reject_overflow() {
        let name = "a".repeat(MAX_NAME);
        let value = "b".repeat(MAX_VALUE);
        let json = serde_json::json!({name.clone(): value.clone()}).to_string();
        assert!(parse(json.as_bytes()).is_ok());
        for json in [
            serde_json::json!({format!("{name}a"): "value"}),
            serde_json::json!({"x": format!("{value}b")}),
        ] {
            assert!(parse(json.to_string().as_bytes()).is_err());
        }
        let mut entries = serde_json::Map::new();
        for index in 0..MAX_ENTRIES {
            entries.insert(format!("x-{index}"), "value".into());
        }
        assert!(parse(serde_json::to_vec(&entries).unwrap().as_slice()).is_ok());
        entries.insert("x-overflow".into(), "value".into());
        assert!(parse(serde_json::to_vec(&entries).unwrap().as_slice()).is_err());
        let mut bytes = b"{}".to_vec();
        bytes.resize(MAX_BYTES, b' ');
        assert!(parse(&bytes).is_ok());
        bytes.push(b' ');
        assert!(parse(&bytes).is_err());
    }
}
