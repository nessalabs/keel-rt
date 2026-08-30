use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

/// Named failpoints: `enable("store.put")`, consume remaining hits with [`take`].
/// Keep this small. Do not scatter failpoints through the production scheduler
/// until a test needs one. Store/executor test doubles check these names.
fn map() -> &'static Mutex<HashMap<String, u32>> {
    static MAP: OnceLock<Mutex<HashMap<String, u32>>> = OnceLock::new();
    MAP.get_or_init(|| Mutex::new(HashMap::new()))
}

pub fn enable(name: &str, hits: u32) {
    map()
        .lock()
        .expect("failpoints")
        .insert(name.to_string(), hits);
}

pub fn disable(name: &str) {
    map().lock().expect("failpoints").remove(name);
}

pub fn reset() {
    map().lock().expect("failpoints").clear();
}

/// Decrement remaining hits. Returns true when this call should inject a fault.
pub fn take(name: &str) -> bool {
    let mut g = map().lock().expect("failpoints");
    match g.get_mut(name) {
        Some(n) if *n > 0 => {
            *n -= 1;
            true
        }
        _ => false,
    }
}

pub fn remaining(name: &str) -> u32 {
    map()
        .lock()
        .expect("failpoints")
        .get(name)
        .copied()
        .unwrap_or(0)
}
