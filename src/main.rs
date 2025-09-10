#![allow(unused_imports)]
use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::{Arc, Mutex};
use std::thread;

fn parse_redis_command(buffer: &[u8], n: usize) -> Option<Vec<String>> {
    let data = String::from_utf8_lossy(&buffer[..n]);
    let lines: Vec<&str> = data.split("\r\n").collect();
    
    if lines.is_empty() || !lines[0].starts_with('*') {
        return None;
    }
    
    let array_len: usize = lines[0][1..].parse().ok()?;
    let mut args = Vec::new();
    let mut line_idx = 1;
    
    for _ in 0..array_len {
        if line_idx >= lines.len() || !lines[line_idx].starts_with('$') {
            return None;
        }
        
        let str_len: usize = lines[line_idx][1..].parse().ok()?;
        line_idx += 1;
        
        if line_idx >= lines.len() {
            return None;
        }
        
        let arg = lines[line_idx].to_string();
        if arg.len() != str_len {
            return None;
        }
        
        args.push(arg);
        line_idx += 1;
    }
    
    Some(args)
}

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    let data_store = Arc::new(Mutex::new(HashMap::<String, String>::new()));

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                let store_clone = Arc::clone(&data_store);
                thread::spawn(move || {
                    let mut buffer = [0; 1024];
                    loop {
                        match stream.read(&mut buffer) {
                            Ok(0) => break,
                            Ok(n) => {
                                if let Some(args) = parse_redis_command(&buffer, n) {
                                    if !args.is_empty() {
                                        let command = args[0].to_uppercase();
                                        match command.as_str() {
                                            "PING" => {
                                                stream.write_all(b"+PONG\r\n").unwrap();
                                            }
                                            "ECHO" => {
                                                if args.len() > 1 {
                                                    let arg = &args[1];
                                                    let response = format!("${}\r\n{}\r\n", arg.len(), arg);
                                                    stream.write_all(response.as_bytes()).unwrap();
                                                }
                                            }
                                            "SET" => {
                                                if args.len() >= 3 {
                                                    let key = &args[1];
                                                    let value = &args[2];
                                                    let mut store = store_clone.lock().unwrap();
                                                    store.insert(key.clone(), value.clone());
                                                    stream.write_all(b"+OK\r\n").unwrap();
                                                }
                                            }
                                            "GET" => {
                                                if args.len() >= 2 {
                                                    let key = &args[1];
                                                    let store = store_clone.lock().unwrap();
                                                    match store.get(key) {
                                                        Some(value) => {
                                                            let response = format!("${}\r\n{}\r\n", value.len(), value);
                                                            stream.write_all(response.as_bytes()).unwrap();
                                                        }
                                                        None => {
                                                            stream.write_all(b"$-1\r\n").unwrap();
                                                        }
                                                    }
                                                }
                                            }
                                            _ => {
                                                stream.write_all(b"+PONG\r\n").unwrap();
                                            }
                                        }
                                    }
                                } else {
                                    stream.write_all(b"+PONG\r\n").unwrap();
                                }
                            }
                            Err(e) => {
                                println!("error reading from stream: {}", e);
                                break;
                            }
                        }
                    }
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
