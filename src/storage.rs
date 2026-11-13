use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::SystemTime;

pub type Storage = Arc<RwLock<HashMap<Vec<u8>, Entry>>>;

#[derive(Debug, Clone)]
pub struct Entry {
    pub value: Vec<u8>,
    pub expires_at: Option<SystemTime>,
}

impl Entry {
    pub fn new(value: Vec<u8>, expires_at: Option<SystemTime>) -> Self {
        Self { value, expires_at }
    }

    pub fn is_expired(&self) -> bool {
        self.expires_at
            .is_some_and(|deadline| deadline <= SystemTime::now())
    }
}
