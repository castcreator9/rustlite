use crate::{
    InputBuffer,
    mem_storage::{EMAIL_SIZE, Row, USERNAME_SIZE},
};

// Meta commands always start with a dot
pub enum MetaCommandResult {
    CommandSuccess,
    CommandUnrecognizedCommand,
}

pub fn do_meta_command(input_buffer: &InputBuffer) -> MetaCommandResult {
    if input_buffer.buffer == ".exit" {
        std::process::exit(0);
    } else {
        return MetaCommandResult::CommandUnrecognizedCommand;
    }
}

pub enum PrepareResult {
    Success,
    UnrecognizedStatement,
    SyntaxError,
}

pub enum StatementType {
    Insert,
    Select,
}

pub struct Statement {
    pub stype: StatementType,
    pub row_to_insert: Row,
}

impl Statement {
    pub fn new() -> Self {
        Self {
            stype: StatementType::Select,
            row_to_insert: Row::new(),
        }
    }

    // Check and parse the user's input
    pub fn prepare_statement(&mut self, input_buffer: &InputBuffer) -> PrepareResult {
        if input_buffer.buffer.len() >= 6 && &input_buffer.buffer[..6] == "insert" {
            self.stype = StatementType::Insert;

            let mut parts = input_buffer.buffer.split_whitespace();
            let _command = parts.next();
            let id = parts.next();
            let username = parts.next();
            let email = parts.next();

            // Check if the arguments are valid
            match (id, username, email) {
                (Some(id), Some(username), Some(email)) => {
                    if username.len() <= USERNAME_SIZE && email.len() <= EMAIL_SIZE {
                        if let Ok(id) = id.parse::<u32>() {
                            let row = Row {
                                id,
                                username: username.to_string(),
                                email: email.to_string(),
                            };
                            self.row_to_insert = row;
                        } else {
                            return PrepareResult::SyntaxError;
                        }
                    } else {
                        return PrepareResult::SyntaxError;
                    }
                }
                _ => {
                    return PrepareResult::SyntaxError;
                }
            }

            return PrepareResult::Success;
        }

        if input_buffer.buffer == "select" {
            self.stype = StatementType::Select;
            return PrepareResult::Success;
        }

        PrepareResult::UnrecognizedStatement
    }
}
