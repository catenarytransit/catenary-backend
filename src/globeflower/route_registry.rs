use std::collections::HashMap;

pub type RouteId = u32;

/// Interns route identifiers (chateau, route_id) to u32 to save memory and reduce cloning.
pub struct RouteRegistry {
    to_id: HashMap<(String, String), RouteId>,
    from_id: Vec<(String, String)>,
}

impl RouteRegistry {
    pub fn new() -> Self {
        Self {
            to_id: HashMap::new(),
            from_id: Vec::new(),
        }
    }

    pub fn get_or_insert(&mut self, chateau: String, route_id: String) -> RouteId {
        let key = (chateau, route_id);
        if let Some(&id) = self.to_id.get(&key) {
            id
        } else {
            let id = self.from_id.len() as RouteId;
            self.from_id.push(key.clone());
            self.to_id.insert(key, id);
            id
        }
    }

    pub fn get(&self, id: RouteId) -> Option<&(String, String)> {
        self.from_id.get(id as usize)
    }

    pub fn get_by_val(&self, chateau: &str, route_id: &str) -> Option<RouteId> {
        self.to_id.get(&(chateau.to_string(), route_id.to_string())).copied()
    }
}
