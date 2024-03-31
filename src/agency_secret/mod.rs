pub struct PasswordFormat {
    key_formats: Vec<KeyFormat>,
    passwords: Vec<PasswordInfo>
}

pub enum KeyFormat {
    Header(String),
    UrlQuery(String)
}

pub struct PasswordInfo {
    pub password: Vec<String>,
    pub creator_email: String,
}