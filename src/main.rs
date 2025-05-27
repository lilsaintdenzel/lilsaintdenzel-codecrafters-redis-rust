#![allow(unused_imports)]
use std::io::{Read, Write};  // Line 2
use std::net::TcpListener;   // Line 3

fn main() {                  // Line 5
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    
    for stream in listener.incoming() {     // Line 8
        match stream {                      // Line 9
            Ok(mut stream) => {             // Line 10
                let mut buffer = [0; 1024]; // Line 11
                
                loop {                      // Line 13
                    match stream.read(&mut buffer) {  // Line 14
                        Ok(0) => break,
                        Ok(_) => {
                            stream.write_all(b"+PONG\r\n").unwrap();
                        }
                        Err(e) => {
                            println!("error reading from stream: {}", e);
                            break;
                        }
                        
                    }  // Line 22: Close inner match
                }  // Line 23: Close loop
            }  // Line 24: Close Ok arm
            Err(e) => {
                println!("error: {}", e);
            }
        }  // Line 28: Close outer match
    }  // Line 29: Close for loop
}  // Line 30: Close main
