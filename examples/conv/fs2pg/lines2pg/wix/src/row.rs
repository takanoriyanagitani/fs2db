pub struct Record {
    pub offset: i64,
    pub id: i64,
    pub title: String,
}

impl Record {
    pub fn to_key_string(&self) -> String {
        format!("{}:{}", self.offset, self.id)
    }

    pub fn to_key_bytes(&self) -> Vec<u8> {
        self.to_key_string().into()
    }

    pub fn into_value(self) -> Vec<u8> {
        self.title.into()
    }

    pub fn into_pair(self) -> (Vec<u8>, Vec<u8>) {
        (self.to_key_bytes(), self.into_value())
    }
}
