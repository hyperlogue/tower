use crate::{Result, error, internal};
use serde_json::{Value, json};
fn safe_schema(value: &Value, depth: usize) -> bool {
    if depth > 16 {
        return false;
    }
    match value {
        Value::Object(fields) => fields.iter().all(|(k, v)| {
            !matches!(k.as_str(), "$ref" | "$dynamicRef" | "$recursiveRef")
                && safe_schema(v, depth + 1)
        }),
        Value::Array(values) => values.iter().all(|v| safe_schema(v, depth + 1)),
        _ => true,
    }
}
pub(crate) fn validator(schema: &Value) -> Result<jsonschema::Validator> {
    if serde_json::to_vec(schema).map_err(internal)?.len() > 32768
        || !safe_schema(schema, 0)
        || schema["type"] != "object"
    {
        return Err(error(
            "unsupported_schema",
            "A bounded object schema without references is required",
        ));
    }
    jsonschema::options()
        .offline()
        .with_pattern_options(jsonschema::PatternOptions::regex())
        .should_validate_formats(true)
        .build(schema)
        .map_err(|_| error("unsupported_schema", "This form schema is not supported"))
}
pub(crate) fn questions(input: &Value, claude: bool) -> Result<Value> {
    let questions = input["questions"]
        .as_array()
        .filter(|v| !v.is_empty() && v.len() <= 16)
        .ok_or_else(|| error("unsupported_schema", "Question list is invalid"))?;
    let mut properties = serde_json::Map::new();
    let mut required = Vec::new();
    for (index, q) in questions.iter().enumerate() {
        if q["isSecret"] == true {
            return Err(error(
                "unsupported_schema",
                "Secret input requires a separate secure interaction",
            ));
        }
        let key = if claude {
            format!("question_{index}")
        } else {
            q["id"]
                .as_str()
                .filter(|s| crate::identifier(s))
                .ok_or_else(|| error("unsupported_schema", "Question identity is invalid"))?
                .into()
        };
        let text = q["question"]
            .as_str()
            .ok_or_else(|| error("unsupported_schema", "Question text is missing"))?;
        let labels: Vec<_> = q["options"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|o| o["label"].as_str().map(str::to_owned))
            .collect();
        let mut field = json!({"type":"string","title":q["header"],"description":text});
        if !labels.is_empty() && q["isOther"] != true {
            field["enum"] = json!(labels);
        }
        if claude && q["multiSelect"] == true {
            field = json!({"type":"array","title":q["header"],"description":text,"items":{"type":"string","enum":labels},"uniqueItems":true,"minItems":1});
        }
        if properties.insert(key.clone(), field).is_some() {
            return Err(error(
                "unsupported_schema",
                "Question identities must be unique",
            ));
        }
        required.push(key);
    }
    let schema = json!({"type":"object","properties":properties,"required":required,"additionalProperties":false});
    validator(&schema)?;
    Ok(
        json!({"mode":"form","message":"The agent needs your input to continue.","requestedSchema":schema}),
    )
}
pub(crate) fn elicitation(method: &str, p: &Value) -> Result<Option<Value>> {
    if method == "item/tool/requestUserInput" {
        return questions(p, false).map(Some);
    }
    if !matches!(
        method,
        "mcpServer/elicitation/request" | "elicitation/create"
    ) {
        return Ok(None);
    }
    let mode = p["mode"].as_str().unwrap_or("form");
    let message = p["message"]
        .as_str()
        .ok_or_else(|| error("native_protocol", "Elicitation message is missing"))?;
    if mode == "url" {
        let raw = p["url"]
            .as_str()
            .ok_or_else(|| error("native_protocol", "Elicitation URL is missing"))?;
        let url = url::Url::parse(raw)
            .map_err(|_| error("native_protocol", "Invalid elicitation URL"))?;
        if raw.len() > 8192
            || raw.chars().any(char::is_control)
            || !url.username().is_empty()
            || url.password().is_some()
            || !(url.scheme() == "https"
                || (url.scheme() == "http"
                    && matches!(
                        url.host_str(),
                        Some("localhost" | "127.0.0.1" | "[::1]" | "::1")
                    )))
        {
            return Err(error(
                "native_protocol",
                "Elicitation URL must use a safe web origin",
            ));
        }
        return Ok(Some(
            json!({"mode":"url","message":message,"url":raw,"elicitationId":p["elicitationId"]}),
        ));
    }
    if !matches!(mode, "form" | "openai/form" | "openaiForm") {
        return Err(error("unsupported_schema", "Unknown elicitation mode"));
    }
    validator(&p["requestedSchema"])?;
    Ok(Some(
        json!({"mode":"form","message":message,"requestedSchema":p["requestedSchema"]}),
    ))
}
pub(crate) fn answer(method: &str, payload: &Value, response: &Value) -> Result<Value> {
    let action = response["action"]
        .as_str()
        .filter(|v| matches!(*v, "accept" | "decline" | "cancel"))
        .ok_or_else(|| error("invalid_response", "Choose accept, decline, or cancel"))?;
    if action == "accept" {
        if payload["mode"] == "form" {
            if !validator(&payload["requestedSchema"])?.is_valid(&response["content"]) {
                return Err(error(
                    "invalid_response",
                    "Response does not match the live request schema",
                ));
            }
        } else if response.get("content").is_some_and(|v| !v.is_null()) {
            return Err(error(
                "invalid_response",
                "URL consent must not return private credentials",
            ));
        }
    }
    if method == "item/tool/requestUserInput" {
        let mut answers = serde_json::Map::new();
        if action == "accept" {
            for (k, v) in response["content"]
                .as_object()
                .ok_or_else(|| error("invalid_response", "Question answers must be an object"))?
            {
                answers.insert(k.clone(),json!({"answers":[v.as_str().ok_or_else(||error("invalid_response","Question answer must be text"))?]}));
            }
        }
        return Ok(json!({"answers":answers}));
    }
    Ok(if action == "accept" && payload["mode"] == "form" {
        json!({"action":action,"content":response["content"]})
    } else {
        json!({"action":action})
    })
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn live_forms_validate_and_cannot_fetch_schemas_or_return_url_secrets() {
        assert!(validator(&json!({"type":"object","$ref":"file:///private"})).is_err());
        let form=questions(&json!({"questions":[{"id":"choice","header":"Choice","question":"Pick one","options":[{"label":"yes"},{"label":"no"}]}]}),false).unwrap();
        assert!(
            answer(
                "item/tool/requestUserInput",
                &form,
                &json!({"action":"accept","content":{"choice":"wrong"}})
            )
            .is_err()
        );
        assert_eq!(
            answer(
                "item/tool/requestUserInput",
                &form,
                &json!({"action":"accept","content":{"choice":"yes"}})
            )
            .unwrap()["answers"]["choice"]["answers"],
            json!(["yes"])
        );
        let url=elicitation("elicitation/create",&json!({"mode":"url","message":"Authorize","url":"https://example.invalid/private?nonce=fixture","elicitationId":"id"})).unwrap().unwrap();
        assert!(
            answer(
                "elicitation/create",
                &url,
                &json!({"action":"accept","content":{"token":"secret"}})
            )
            .is_err()
        );
        assert_eq!(
            answer("elicitation/create", &url, &json!({"action":"accept"})).unwrap(),
            json!({"action":"accept"})
        );
        assert!(
            questions(
                &json!({"questions":[{"id":"secret","isSecret":true,"question":"Password"}]}),
                false
            )
            .is_err()
        );
    }
}
