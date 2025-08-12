use crate::{error::Result, RocksMap};
use serde::{de::DeserializeOwned, Serialize};
use std::{collections::HashSet, marker::PhantomData, path::Path};

/// A trait that allows extraction of secondary keys from a value
pub trait IndexExtractor<V, SK> {
    /// Extract a secondary key from a value
    fn extract(value: &V) -> SK;
}

/// A secondary index that maps from a secondary key type to the primary key type
pub struct SecondaryIndex<PK, V, SK, E>
where
    PK: Clone + Serialize + DeserializeOwned + std::hash::Hash + std::cmp::Eq,
    V: Clone + Serialize + DeserializeOwned,
    SK: Clone + Serialize + DeserializeOwned,
    E: IndexExtractor<V, SK>,
{
    main_store: RocksMap<PK, V>,
    index_store: RocksMap<SK, HashSet<PK>>,
    _extractor: PhantomData<E>,
}

impl<PK, V, SK, E> SecondaryIndex<PK, V, SK, E>
where
    PK: Clone + Serialize + DeserializeOwned + Eq + std::hash::Hash,
    V: Clone + Serialize + DeserializeOwned,
    SK: Clone + Serialize + DeserializeOwned,
    E: IndexExtractor<V, SK>,
{
    /// Create a new secondary index
    pub fn new<P: AsRef<Path>>(path: P, index_name: &str) -> Result<Self> {
        let main_path = path.as_ref().to_path_buf();
        let index_path = main_path.join(format!("{}_index", index_name));

        let main_store = RocksMap::<PK, V>::open(main_path)?;
        let index_store = RocksMap::<SK, HashSet<PK>>::open(index_path)?;

        Ok(Self {
            main_store,
            index_store,
            _extractor: PhantomData,
        })
    }

    /// Store a value with automatic index updating
    pub fn put(&self, key: PK, value: &V) -> Result<()> {
        let secondary_key = E::extract(value);

        self.main_store.put(key.clone(), value)?;

        let mut primary_keys = self
            .index_store
            .get(&secondary_key)?
            .unwrap_or_else(HashSet::new);
        primary_keys.insert(key);
        self.index_store.put(secondary_key, &primary_keys)?;

        Ok(())
    }

    /// Get a value by primary key
    pub fn get(&self, key: &PK) -> Result<Option<V>> {
        self.main_store.get(key)
    }

    /// Find all values that match a secondary key
    pub fn find_by_secondary_key(&self, secondary_key: &SK) -> Result<Vec<V>> {
        let primary_keys = match self.index_store.get(secondary_key)? {
            Some(keys) => keys,
            None => return Ok(Vec::new()),
        };

        let mut results = Vec::with_capacity(primary_keys.len());
        for pk in primary_keys {
            if let Some(value) = self.main_store.get(&pk)? {
                results.push(value);
            }
        }

        Ok(results)
    }

    /// Delete a value and update the index
    pub fn delete(&self, key: &PK) -> Result<()> {
        let value = match self.main_store.get(key)? {
            Some(v) => v,
            None => return Ok(()),
        };

        let secondary_key = E::extract(&value);

        if let Some(mut primary_keys) = self.index_store.get(&secondary_key)? {
            primary_keys.remove(key);

            if primary_keys.is_empty() {
                self.index_store.delete(&secondary_key)?;
            } else {
                self.index_store.put(secondary_key, &primary_keys)?;
            }
        }

        self.main_store.delete(key)?;

        Ok(())
    }

    /// Get access to the underlying main store
    pub fn main_store(&self) -> &RocksMap<PK, V> {
        &self.main_store
    }

    /// Get access to the underlying index store
    pub fn index_store(&self) -> &RocksMap<SK, HashSet<PK>> {
        &self.index_store
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};
    use tempfile::TempDir;

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct User {
        id: u64,
        name: String,
        email: String,
        role: String,
    }

    struct EmailExtractor;
    impl IndexExtractor<User, String> for EmailExtractor {
        fn extract(user: &User) -> String {
            user.email.clone()
        }
    }

    struct RoleExtractor;
    impl IndexExtractor<User, String> for RoleExtractor {
        fn extract(user: &User) -> String {
            user.role.clone()
        }
    }

    #[test]
    fn test_secondary_index() {
        let temp_dir = TempDir::new().unwrap();
        let index = SecondaryIndex::<u64, User, String, EmailExtractor>::new(
            temp_dir.path(),
            "email_index",
        )
        .unwrap();

        let users = vec![
            User {
                id: 1,
                name: "Alice".to_string(),
                email: "alice@example.com".to_string(),
                role: "admin".to_string(),
            },
            User {
                id: 2,
                name: "Bob".to_string(),
                email: "bob@example.com".to_string(),
                role: "user".to_string(),
            },
            User {
                id: 3,
                name: "Charlie".to_string(),
                email: "charlie@example.com".to_string(),
                role: "user".to_string(),
            },
        ];

        for user in &users {
            index.put(user.id, user).unwrap();
        }

        let alice = index.get(&1).unwrap().unwrap();
        assert_eq!(alice.name, "Alice");

        let bob_result = index
            .find_by_secondary_key(&"bob@example.com".to_string())
            .unwrap();
        assert_eq!(bob_result.len(), 1);
        assert_eq!(bob_result[0].name, "Bob");

        index.delete(&2).unwrap();

        assert!(index.get(&2).unwrap().is_none());

        let empty_result = index
            .find_by_secondary_key(&"bob@example.com".to_string())
            .unwrap();
        assert_eq!(empty_result.len(), 0);
    }

    #[test]
    fn test_multiple_values_same_index() {
        let temp_dir = TempDir::new().unwrap();
        let index =
            SecondaryIndex::<u64, User, String, RoleExtractor>::new(temp_dir.path(), "role_index")
                .unwrap();

        let users = vec![
            User {
                id: 1,
                name: "Alice".to_string(),
                email: "alice@example.com".to_string(),
                role: "admin".to_string(),
            },
            User {
                id: 2,
                name: "Bob".to_string(),
                email: "bob@example.com".to_string(),
                role: "user".to_string(),
            },
            User {
                id: 3,
                name: "Charlie".to_string(),
                email: "charlie@example.com".to_string(),
                role: "user".to_string(),
            },
        ];

        for user in &users {
            index.put(user.id, user).unwrap();
        }

        let users_result = index.find_by_secondary_key(&"user".to_string()).unwrap();
        assert_eq!(users_result.len(), 2);

        let names: Vec<String> = users_result.iter().map(|u| u.name.clone()).collect();
        assert!(names.contains(&"Bob".to_string()));
        assert!(names.contains(&"Charlie".to_string()));

        let admins = index.find_by_secondary_key(&"admin".to_string()).unwrap();
        assert_eq!(admins.len(), 1);
        assert_eq!(admins[0].name, "Alice");
    }
}
