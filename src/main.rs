use std::io::{self, Write};

pub mod mem_storage;
pub mod tokenizer;

use crate::mem_storage::{ExecuteResult, Table};
use crate::tokenizer::{MetaCommandResult, PrepareResult, Statement, do_meta_command};

pub struct InputBuffer {
    buffer: String,
    input_lenght: usize,
}

impl InputBuffer {
    pub fn new() -> Self {
        Self {
            buffer: String::new(),
            input_lenght: 0,
        }
    }

    pub fn read_input(&mut self) {
        print!("db > ");
        io::stdout().flush().unwrap();

        self.buffer.clear();
        io::stdin()
            .read_line(&mut self.buffer)
            .expect("Failed to read line");

        // Remove the last char -> \n
        self.buffer.pop().unwrap();
        self.input_lenght = self.buffer.trim_end().len();
    }
}

fn main() {
    let mut table = Table::new();
    let mut input_buffer = InputBuffer::new();

    loop {
        input_buffer.read_input();
        if input_buffer.buffer.starts_with('.') {
            match do_meta_command(&input_buffer) {
                MetaCommandResult::CommandSuccess => {
                    continue;
                }
                MetaCommandResult::CommandUnrecognizedCommand => {
                    println!("Unrecognized command '{}'", input_buffer.buffer);
                    continue;
                }
            }
        }

        let mut statement = Statement::new();
        match statement.prepare_statement(&input_buffer) {
            PrepareResult::Success => {}
            PrepareResult::UnrecognizedStatement => {
                println!(
                    "Unrecognized keyword at start of '{}'.",
                    input_buffer.buffer
                );
                continue;
            }
            PrepareResult::SyntaxError => {
                println!("Syntax error. Could not parse the statement.");
                continue;
            }
            PrepareResult::StringTooLong => {
                println!("String is too long.");
                continue;
            }
            PrepareResult::IdIssue => {
                println!("Id must be a positive integer.");
                continue;
            }
        }

        match table.execute_statement(&statement) {
            ExecuteResult::Success => {
                println!("Executed.");
            }
            ExecuteResult::TableFull => {
                println!("Error: Table full.");
            }
        }
    }
}
