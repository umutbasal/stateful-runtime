use std::collections::HashMap;

use anyhow::{anyhow, Result};
use axum::routing::MethodFilter;

pub fn method_filter(method: &str) -> Result<MethodFilter> {
    match method.to_uppercase().as_str() {
        "GET" => Ok(MethodFilter::GET),
        "POST" => Ok(MethodFilter::POST),
        "PUT" => Ok(MethodFilter::PUT),
        "PATCH" => Ok(MethodFilter::PATCH),
        "DELETE" => Ok(MethodFilter::DELETE),
        "HEAD" => Ok(MethodFilter::HEAD),
        "OPTIONS" => Ok(MethodFilter::OPTIONS),
        other => Err(anyhow!("unsupported HTTP method '{other}'")),
    }
}

pub fn parse_query_string(query: Option<&str>) -> HashMap<String, String> {
    let mut output = HashMap::new();
    if let Some(query) = query {
        for pair in query.split('&') {
            if pair.is_empty() {
                continue;
            }
            let mut parts = pair.splitn(2, '=');
            let key = parts.next().unwrap_or_default();
            let value = parts.next().unwrap_or_default();
            output.insert(key.to_string(), value.to_string());
        }
    }
    output
}
