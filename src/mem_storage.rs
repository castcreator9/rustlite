use crate::tokenizer::{Statement, StatementType};

pub enum ExecuteResult {
    Success,
    TableFull,
}

const ID_SIZE: usize = 4;
pub const USERNAME_SIZE: usize = 32;
pub const EMAIL_SIZE: usize = 255;

const ID_OFFSET: usize = 0;
const USERNAME_OFFSET: usize = ID_OFFSET + ID_SIZE;
const EMAIL_OFFSET: usize = USERNAME_OFFSET + USERNAME_SIZE;
const ROW_SIZE: usize = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

pub struct Row {
    pub id: u32,
    pub username: String,
    pub email: String,
}

impl Row {
    pub fn new() -> Self {
        Self {
            id: 0,
            username: String::new(),
            email: String::new(),
        }
    }

    pub fn serialize_row(&self) -> Vec<u8> {
        let mut buffer = Vec::with_capacity(ID_SIZE + USERNAME_SIZE + EMAIL_SIZE);
        buffer.extend(self.id.to_le_bytes());

        let mut username_bytes = [0u8; USERNAME_SIZE];
        username_bytes[..self.username.len()].copy_from_slice(self.username.as_bytes());
        buffer.extend(&username_bytes);

        let mut email_bytes = [0u8; EMAIL_SIZE];
        email_bytes[..self.email.len()].copy_from_slice(self.email.as_bytes());
        buffer.extend(&email_bytes);

        buffer
    }

    pub fn deserialize_row(buffer: &[u8]) -> Option<Self> {
        if buffer.len() < ID_SIZE + USERNAME_SIZE + EMAIL_SIZE {
            return None;
        }

        let id = u32::from_le_bytes(buffer[ID_OFFSET..ID_SIZE].try_into().ok()?);
        let username_bytes = &buffer[USERNAME_OFFSET..USERNAME_OFFSET + USERNAME_SIZE];
        let email_bytes = &buffer[EMAIL_OFFSET..EMAIL_OFFSET + EMAIL_SIZE];

        let username = String::from_utf8(username_bytes.to_vec()).ok()?;
        let email = String::from_utf8(email_bytes.to_vec()).ok()?;

        Some(Row {
            id,
            username,
            email,
        })
    }
}

const PAGE_SIZE: usize = 4096;
const TABLE_MAX_PAGES: usize = 100;
const ROWS_PER_PAGE: usize = PAGE_SIZE / ROW_SIZE;
const TABLE_MAX_ROWS: usize = ROWS_PER_PAGE * TABLE_MAX_PAGES;

pub struct Table {
    pages: Box<Vec<Vec<u8>>>,
    num_rows: usize,
}

impl Table {
    pub fn new() -> Self {
        Self {
            pages: Box::new(Vec::new()),
            num_rows: 0,
        }
    }

    fn execute_insert(&mut self, statement: &Statement) -> ExecuteResult {
        if self.num_rows >= TABLE_MAX_ROWS {
            return ExecuteResult::TableFull;
        }

        let serialized_data = statement.row_to_insert.serialize_row();

        let row_num = self.num_rows;
        let page_num = row_num / ROWS_PER_PAGE;
        let row_offset = (row_num % ROWS_PER_PAGE) * ROW_SIZE;

        // Initialize page
        if self.pages.len() <= page_num {
            while self.pages.len() <= page_num {
                self.pages.push(vec![0; PAGE_SIZE]);
            }
        }

        let page = &mut self.pages[page_num];
        page[row_offset..row_offset + ROW_SIZE].copy_from_slice(&serialized_data);
        self.num_rows += 1;

        ExecuteResult::Success
    }

    fn execute_select(&self) -> ExecuteResult {
        for row_num in 0..self.num_rows {
            let page_num = row_num / ROWS_PER_PAGE;
            let row_offset = (row_num % ROWS_PER_PAGE) * ROW_SIZE;

            let page = &self.pages[page_num];
            let row_data = &page[row_offset..row_offset + ROW_SIZE];

            if let Some(row) = Row::deserialize_row(row_data) {
                println!("({}, {}, {})", row.id, row.username, row.email,);
            } else {
                println!("Error deserializing data.");
            }
        }

        ExecuteResult::Success
    }

    pub fn execute_statement(&mut self, statement: &Statement) -> ExecuteResult {
        match statement.stype {
            StatementType::Insert => self.execute_insert(statement),
            StatementType::Select => self.execute_select(),
        }
    }
}
