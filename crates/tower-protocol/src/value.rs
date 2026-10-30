use serde::{
    Deserialize, Deserializer, Serialize,
    de::{MapAccess, SeqAccess, Visitor},
};
use std::{collections::BTreeMap, fmt};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Number {
    I64(i64),
    U64(u64),
    F64(f64),
}

/// JSON data independent of any particular codec. Preserve integer precision.
#[derive(Clone, Debug, Default, PartialEq, Serialize)]
#[serde(untagged)]
pub enum Value {
    #[default]
    Null,
    Bool(bool),
    Number(Number),
    String(String),
    Array(Vec<Value>),
    Object(BTreeMap<String, Value>),
}
impl<'de> Deserialize<'de> for Value {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct JsonVisitor;
        impl<'de> Visitor<'de> for JsonVisitor {
            type Value = Value;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a JSON value")
            }
            fn visit_unit<E: serde::de::Error>(self) -> Result<Value, E> {
                Ok(Value::Null)
            }
            fn visit_none<E: serde::de::Error>(self) -> Result<Value, E> {
                Ok(Value::Null)
            }
            fn visit_bool<E: serde::de::Error>(self, v: bool) -> Result<Value, E> {
                Ok(Value::Bool(v))
            }
            fn visit_i64<E: serde::de::Error>(self, v: i64) -> Result<Value, E> {
                Ok(Value::Number(Number::I64(v)))
            }
            fn visit_u64<E: serde::de::Error>(self, v: u64) -> Result<Value, E> {
                Ok(Value::Number(Number::U64(v)))
            }
            fn visit_f64<E: serde::de::Error>(self, v: f64) -> Result<Value, E> {
                Ok(Value::Number(Number::F64(v)))
            }
            fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Value, E> {
                Ok(Value::String(v.into()))
            }
            fn visit_string<E: serde::de::Error>(self, v: String) -> Result<Value, E> {
                Ok(Value::String(v))
            }
            fn visit_seq<A: SeqAccess<'de>>(self, mut a: A) -> Result<Value, A::Error> {
                let mut out = Vec::new();
                while let Some(v) = a.next_element()? {
                    out.push(v);
                }
                Ok(Value::Array(out))
            }
            fn visit_map<A: MapAccess<'de>>(self, mut a: A) -> Result<Value, A::Error> {
                let mut out = BTreeMap::new();
                while let Some((k, v)) = a.next_entry()? {
                    out.insert(k, v);
                }
                Ok(Value::Object(out))
            }
        }
        d.deserialize_any(JsonVisitor)
    }
}
