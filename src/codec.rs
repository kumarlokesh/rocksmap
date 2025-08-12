use crate::error::{Error, Result};
use serde::{de::DeserializeOwned, Serialize};
use std::marker::PhantomData;

/// Trait defining how to encode a key for storage
pub trait KeyCodec<K> {
    /// Convert a key to bytes for storage
    fn encode(key: &K) -> Result<Vec<u8>>;

    /// Convert bytes back to a key
    fn decode(bytes: &[u8]) -> Result<K>;
}

/// Trait defining how to encode a value for storage
pub trait ValueCodec<V> {
    /// Convert a value to bytes for storage
    fn encode(value: &V) -> Result<Vec<u8>>;

    /// Convert bytes back to a value
    fn decode(bytes: &[u8]) -> Result<V>;
}

/// Default implementation using bincode for serialization
pub struct BincodeCodec<T>(PhantomData<T>);

impl<K> KeyCodec<K> for BincodeCodec<K>
where
    K: Serialize + DeserializeOwned,
{
    fn encode(key: &K) -> Result<Vec<u8>> {
        bincode::serialize(key).map_err(|e| Error::Serialization(e.to_string()))
    }

    fn decode(bytes: &[u8]) -> Result<K> {
        bincode::deserialize(bytes).map_err(|e| Error::Deserialization(e.to_string()))
    }
}

impl<V> ValueCodec<V> for BincodeCodec<V>
where
    V: Serialize + DeserializeOwned,
{
    fn encode(value: &V) -> Result<Vec<u8>> {
        bincode::serialize(value).map_err(|e| Error::Serialization(e.to_string()))
    }

    fn decode(bytes: &[u8]) -> Result<V> {
        bincode::deserialize(bytes).map_err(|e| Error::Deserialization(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct TestKey {
        id: u32,
        name: String,
    }

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct TestValue {
        data: Vec<u8>,
        flag: bool,
    }

    #[test]
    fn test_bincode_key_codec() {
        let key = TestKey {
            id: 42,
            name: "test_key".to_string(),
        };

        let encoded = <BincodeCodec<TestKey> as KeyCodec<TestKey>>::encode(&key).unwrap();
        let decoded = <BincodeCodec<TestKey> as KeyCodec<TestKey>>::decode(&encoded).unwrap();

        assert_eq!(key, decoded);
    }

    #[test]
    fn test_bincode_value_codec() {
        let value = TestValue {
            data: vec![1, 2, 3, 4],
            flag: true,
        };

        let encoded = <BincodeCodec<TestValue> as ValueCodec<TestValue>>::encode(&value).unwrap();
        let decoded = <BincodeCodec<TestValue> as ValueCodec<TestValue>>::decode(&encoded).unwrap();

        assert_eq!(value, decoded);
    }
}
