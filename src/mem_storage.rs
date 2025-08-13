use std::{
    cell::{RefCell, RefMut},
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    rc::Rc,
};

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

pub struct Pager {
    file: File,
    file_length: usize,
    pages: [Option<Box<[u8; PAGE_SIZE]>>; TABLE_MAX_PAGES],
}

impl Pager {
    pub fn pager_open(filename: &str) -> Self {
        let file = match OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .open(filename)
        {
            Ok(f) => f,
            Err(_) => {
                println!("Unable to open file.");
                std::process::exit(0);
            }
        };

        let metadata = match file.metadata() {
            Ok(m) => m,
            Err(_) => {
                println!("Unable to get metadata.");
                std::process::exit(0);
            }
        };

        let file_length = metadata.len() as usize;

        Self {
            file,
            file_length,
            pages: std::array::from_fn(|_| None),
        }
    }

    pub fn get_page_mut(&mut self, page_num: usize) -> &mut [u8; PAGE_SIZE] {
        if page_num > TABLE_MAX_PAGES {
            println!(
                "Tried to fetch page number out of bounds. {} > {}",
                page_num, TABLE_MAX_PAGES
            );
            std::process::exit(0);
        }

        if self.pages[page_num].is_none() {
            // Allocate memory and load from file
            self.pages[page_num] = Some(Box::new([0u8; PAGE_SIZE]));
            let mut num_pages = self.file_length / PAGE_SIZE;

            // We might save a partial page at the end of the file
            if self.file_length % PAGE_SIZE != 0 {
                num_pages += 1;
            }

            if page_num <= num_pages {
                // Move the cursor and read
                let offset = (page_num * PAGE_SIZE) as u64;
                let _ = self.file.seek(SeekFrom::Start(offset));
                let _ = self
                    .file
                    .read_exact(self.pages[page_num].as_deref_mut().unwrap());
            }
        }

        self.pages[page_num].as_deref_mut().unwrap()
    }

    fn flush(&mut self, page_num: usize, size: usize) {
        if self.pages[page_num].is_none() {
            println!("Tried to flush null page.");
            std::process::exit(0);
        }

        let page = self
            .pages
            .get(page_num)
            .and_then(|p| p.as_ref())
            .expect("Tried to flush null page.");

        let offset = (page_num * PAGE_SIZE) as u64;
        let _ = self.file.seek(SeekFrom::Start(offset));
        let _ = self.file.write_all(&page[..size]);
    }
}

pub struct Table {
    pager: Pager,
    num_rows: usize,
}

type TableRef = Rc<RefCell<Table>>;

pub struct Cursor {
    table: TableRef,
    row_num: usize,
    end_of_table: bool,
}

impl Cursor {
    pub fn from_start(table: TableRef) -> Self {
        let num_rows = table.borrow().num_rows;
        Self {
            table,
            row_num: 0,
            end_of_table: (num_rows == 0),
        }
    }

    pub fn from_end(table: TableRef) -> Self {
        let num_rows = table.borrow().num_rows;
        Self {
            table,
            row_num: num_rows,
            end_of_table: true,
        }
    }

    pub fn get_value(&self) -> RefMut<[u8; PAGE_SIZE]> {
        let row_num = self.row_num;
        let page_num = row_num / ROWS_PER_PAGE;

        RefMut::map(self.table.borrow_mut(), |table| {
            table.get_page_mut(page_num)
        })
    }

    pub fn advance(&mut self) {
        self.row_num += 1;
        if self.row_num >= self.table.borrow().num_rows {
            self.end_of_table = true;
        }
    }
}

impl Table {
    pub fn db_open(filename: &str) -> Self {
        let pager = Pager::pager_open(filename);
        let num_rows = pager.file_length / ROW_SIZE;

        Self {
            pager: pager,
            num_rows,
        }
    }

    // Flushes the page cache to disk
    // Closes the database file
    // Frees the memory for the pager and table data structures
    pub fn db_close(&mut self) {
        let pager = &mut self.pager;
        let num_full_pages = self.num_rows / ROWS_PER_PAGE;

        for i in 0..num_full_pages {
            if pager.pages[i].is_none() {
                continue;
            }
            pager.flush(i, PAGE_SIZE);
            pager.pages[i] = None;
        }

        // There may be a partial page to write to end of the file
        let num_additional_rows = self.num_rows % ROWS_PER_PAGE;
        if num_additional_rows > 0 {
            let page_num = num_full_pages;
            if !pager.pages[page_num].is_none() {
                pager.flush(page_num, num_additional_rows * ROW_SIZE);
                pager.pages[page_num] = None;
            }
        }

        for i in 0..TABLE_MAX_PAGES {
            if !pager.pages[i].is_none() {
                pager.pages[i] = None;
            }
        }
    }

    fn get_page_mut(&mut self, page_num: usize) -> &mut [u8; PAGE_SIZE] {
        self.pager.get_page_mut(page_num)
    }
}

fn execute_insert(table: TableRef, statement: &Statement) -> ExecuteResult {
    {
        if table.borrow().num_rows >= TABLE_MAX_ROWS {
            return ExecuteResult::TableFull;
        }
    }

    let serialized_data = statement.row_to_insert.serialize_row();
    let cursor = Cursor::from_end(Rc::clone(&table));

    let row_offset = (cursor.row_num % ROWS_PER_PAGE) * ROW_SIZE;
    {
        let mut page = cursor.get_value();
        page[row_offset..row_offset + ROW_SIZE].copy_from_slice(&serialized_data);
    }
    {
        table.borrow_mut().num_rows += 1;
    }

    ExecuteResult::Success
}

fn execute_select(table: TableRef) -> ExecuteResult {
    let mut cursor = Cursor::from_start(Rc::clone(&table));

    while !cursor.end_of_table {
        {
            let row_offset = (cursor.row_num % ROWS_PER_PAGE) * ROW_SIZE;
            let page = cursor.get_value();
            let row_data = &page[row_offset..row_offset + ROW_SIZE];

            if let Some(row) = Row::deserialize_row(row_data) {
                println!("({}, {}, {})", row.id, row.username, row.email);
            } else {
                println!("Error deserializing data.");
            }
        }

        cursor.advance();
    }

    ExecuteResult::Success
}

pub fn execute_statement(table: TableRef, statement: &Statement) -> ExecuteResult {
    match statement.stype {
        StatementType::Insert => execute_insert(Rc::clone(&table), statement),
        StatementType::Select => execute_select(Rc::clone(&table)),
    }
}
