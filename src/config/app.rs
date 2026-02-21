use serde::{Deserialize, Serialize};

fn default_max_request_bytes() -> usize {
    1024 * 1024
}

fn default_max_response_bytes() -> usize {
    1024 * 1024
}

fn default_pool_size() -> usize {
    4
}

fn default_max_cpu_ms_per_request() -> u64 {
    5
}

fn default_js_memory_limit_bytes() -> usize {
    128 * 1024 * 1024
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    pub app: AppMetadata,
    pub limits: LimitsConfig,
    #[serde(default)]
    pub formats: FormatsConfig,
    #[serde(default)]
    pub ingestion: Vec<IngestionSource>,
    #[serde(default)]
    pub endpoints: Vec<EndpointConfig>,
    #[serde(default)]
    pub admin: AdminConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppMetadata {
    pub name: String,
    pub version: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LimitsConfig {
    pub max_concurrent_requests: usize,
    #[serde(default)]
    pub rate_limits: RateLimitsConfig,
    pub js: JsLimitsConfig,
    pub store: StoreLimitsConfig,
    #[serde(default = "default_max_request_bytes")]
    pub max_request_bytes: usize,
    #[serde(default = "default_max_response_bytes")]
    pub max_response_bytes: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RateLimitsConfig {
    #[serde(default)]
    pub query_rps: usize,
    #[serde(default)]
    pub ingest_rps: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsLimitsConfig {
    #[serde(default = "default_pool_size")]
    pub pool_size: usize,
    #[serde(default = "default_js_memory_limit_bytes")]
    pub memory_limit_bytes: usize,
    #[serde(default = "default_max_cpu_ms_per_request")]
    pub max_cpu_ms_per_request: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoreLimitsConfig {
    pub max_bytes: usize,
    #[serde(default)]
    pub max_entity_bytes: usize,
    #[serde(default)]
    pub tombstone_ttl_seconds: u64,
    #[serde(default)]
    pub soft_limit_percent: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct FormatsConfig {
    #[serde(default)]
    pub kafka: KafkaFormatConfig,
    #[serde(default)]
    pub http: HttpFormatConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaFormatConfig {
    #[serde(default = "default_kafka_encoding")]
    pub encoding: String,
}

fn default_kafka_encoding() -> String {
    "json".to_string()
}

impl Default for KafkaFormatConfig {
    fn default() -> Self {
        Self {
            encoding: default_kafka_encoding(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpFormatConfig {
    #[serde(default = "default_http_request_encoding")]
    pub request: String,
    #[serde(default = "default_http_response_encoding")]
    pub response: String,
}

fn default_http_request_encoding() -> String {
    "json".to_string()
}

fn default_http_response_encoding() -> String {
    "json".to_string()
}

impl Default for HttpFormatConfig {
    fn default() -> Self {
        Self {
            request: default_http_request_encoding(),
            response: default_http_response_encoding(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestionSource {
    #[serde(rename = "type")]
    pub source_type: String,
    #[serde(default)]
    pub group_id: Option<String>,
    #[serde(default)]
    pub brokers: Vec<String>,
    #[serde(default)]
    pub topics: Vec<IngestionTopicConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestionTopicConfig {
    pub name: String,
    pub entity: String,
    #[serde(default)]
    pub event_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EndpointConfig {
    pub name: String,
    pub method: String,
    pub path: String,
    pub handler: String,
    #[serde(default)]
    pub timeout_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AdminConfig {
    #[serde(default)]
    pub enable_grpc: bool,
    #[serde(default)]
    pub enable_execute: bool,
    #[serde(default)]
    pub bind: Option<String>,
}
