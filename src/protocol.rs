use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChatMessage {
    pub role: String,
    pub content: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Inbound {
    Generate {
        request_id: String,
        model: String,
        messages: Vec<ChatMessage>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        options: Option<Value>,
    },
    ListModels {
        request_id: String,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Outbound {
    Hello {
        client_id: String,
        version: String,
    },
    Token {
        request_id: String,
        content: String,
    },
    Done {
        request_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        stats: Option<Stats>,
    },
    Models {
        request_id: String,
        models: Vec<ModelInfo>,
    },
    Error {
        request_id: String,
        message: String,
    },
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct Stats {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total_duration_ns: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub eval_count: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ModelInfo {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub modified_at: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn round_trip_inbound(v: &Inbound) {
        let s = serde_json::to_string(v).unwrap();
        let back: Inbound = serde_json::from_str(&s).unwrap();
        assert_eq!(*v, back);
    }

    fn round_trip_outbound(v: &Outbound) {
        let s = serde_json::to_string(v).unwrap();
        let back: Outbound = serde_json::from_str(&s).unwrap();
        assert_eq!(*v, back);
    }

    #[test]
    fn parses_generate_request() {
        let raw = json!({
            "type": "generate",
            "request_id": "abc",
            "model": "llama3:8b",
            "messages": [{ "role": "user", "content": "Hi" }],
            "options": { "temperature": 0.7 }
        });
        let parsed: Inbound = serde_json::from_value(raw).unwrap();
        match &parsed {
            Inbound::Generate { request_id, model, messages, options } => {
                assert_eq!(request_id, "abc");
                assert_eq!(model, "llama3:8b");
                assert_eq!(messages.len(), 1);
                assert!(options.is_some());
            }
            _ => panic!("wrong variant"),
        }
        round_trip_inbound(&parsed);
    }

    #[test]
    fn parses_generate_without_options() {
        let raw = json!({
            "type": "generate",
            "request_id": "abc",
            "model": "llama3:8b",
            "messages": [{ "role": "user", "content": "Hi" }]
        });
        let parsed: Inbound = serde_json::from_value(raw).unwrap();
        if let Inbound::Generate { options, .. } = &parsed {
            assert!(options.is_none());
        } else {
            panic!("wrong variant");
        }
    }

    #[test]
    fn parses_list_models_request() {
        let raw = json!({ "type": "list_models", "request_id": "xyz" });
        let parsed: Inbound = serde_json::from_value(raw).unwrap();
        assert_eq!(parsed, Inbound::ListModels { request_id: "xyz".into() });
        round_trip_inbound(&parsed);
    }

    #[test]
    fn serializes_hello() {
        let m = Outbound::Hello { client_id: "ws-1".into(), version: "0.1.0".into() };
        let v: Value = serde_json::from_str(&serde_json::to_string(&m).unwrap()).unwrap();
        assert_eq!(v["type"], "hello");
        assert_eq!(v["client_id"], "ws-1");
        round_trip_outbound(&m);
    }

    #[test]
    fn serializes_token() {
        let m = Outbound::Token { request_id: "abc".into(), content: "Hel".into() };
        let v: Value = serde_json::from_str(&serde_json::to_string(&m).unwrap()).unwrap();
        assert_eq!(v["type"], "token");
        round_trip_outbound(&m);
    }

    #[test]
    fn serializes_done_with_and_without_stats() {
        let m1 = Outbound::Done { request_id: "abc".into(), stats: None };
        let s = serde_json::to_string(&m1).unwrap();
        assert!(!s.contains("stats"), "stats should be skipped when None: {s}");
        round_trip_outbound(&m1);

        let m2 = Outbound::Done {
            request_id: "abc".into(),
            stats: Some(Stats { total_duration_ns: Some(123), eval_count: Some(42) }),
        };
        round_trip_outbound(&m2);
    }

    #[test]
    fn serializes_models() {
        let m = Outbound::Models {
            request_id: "xyz".into(),
            models: vec![ModelInfo {
                name: "llama3:8b".into(),
                size: Some(4_700_000_000),
                modified_at: Some("2024-01-01T00:00:00Z".into()),
            }],
        };
        let v: Value = serde_json::from_str(&serde_json::to_string(&m).unwrap()).unwrap();
        assert_eq!(v["type"], "models");
        assert_eq!(v["models"][0]["name"], "llama3:8b");
        round_trip_outbound(&m);
    }

    #[test]
    fn serializes_error() {
        let m = Outbound::Error { request_id: "abc".into(), message: "model not found".into() };
        let v: Value = serde_json::from_str(&serde_json::to_string(&m).unwrap()).unwrap();
        assert_eq!(v["type"], "error");
        round_trip_outbound(&m);
    }
}
