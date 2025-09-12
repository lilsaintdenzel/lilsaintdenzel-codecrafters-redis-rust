#![allow(unused_imports)]
use std::collections::HashMap;
use std::env;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Clone)]
struct StoredValue {
    value: String,
    expires_at: Option<u128>, // milliseconds since epoch
}

#[derive(Clone)]
struct Config {
    dir: String,
    dbfilename: String,
}

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

fn parse_args() -> Config {
    let args: Vec<String> = env::args().collect();
    let mut dir = "/tmp/redis-data".to_string();
    let mut dbfilename = "dump.rdb".to_string();
    
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--dir" => {
                if i + 1 < args.len() {
                    dir = args[i + 1].clone();
                    i += 2;
                } else {
                    i += 1;
                }
            }
            "--dbfilename" => {
                if i + 1 < args.len() {
                    dbfilename = args[i + 1].clone();
                    i += 2;
                } else {
                    i += 1;
                }
            }
            _ => {
                i += 1;
            }
        }
    }
    
    Config { dir, dbfilename }
}

fn main() {
    let config = parse_args();
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    let data_store = Arc::new(Mutex::new(HashMap::<String, StoredValue>::new()));

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                let store_clone = Arc::clone(&data_store);
                let config_clone = config.clone();
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
                                                    
                                                    let mut expires_at = None;
                                                    
                                                    // Check for expiration options (PX milliseconds)
                                                    if args.len() >= 5 {
                                                        let option = args[3].to_uppercase();
                                                        if option == "PX" {
                                                            if let Ok(ms) = args[4].parse::<u128>() {
                                                                let now = SystemTime::now()
                                                                    .duration_since(UNIX_EPOCH)
                                                                    .unwrap()
                                                                    .as_millis();
                                                                expires_at = Some(now + ms);
                                                            }
                                                        }
                                                    }
                                                    
                                                    let stored_value = StoredValue {
                                                        value: value.clone(),
                                                        expires_at,
                                                    };
                                                    
                                                    let mut store = store_clone.lock().unwrap();
                                                    store.insert(key.clone(), stored_value);
                                                    stream.write_all(b"+OK\r\n").unwrap();
                                                }
                                            }
                                            "GET" => {
                                                if args.len() >= 2 {
                                                    let key = &args[1];
                                                    let mut store = store_clone.lock().unwrap();
                                                    
                                                    match store.get(key) {
                                                        Some(stored_value) => {
                                                            // Check if the key has expired
                                                            if let Some(expires_at) = stored_value.expires_at {
                                                                let now = SystemTime::now()
                                                                    .duration_since(UNIX_EPOCH)
                                                                    .unwrap()
                                                                    .as_millis();
                                                                
                                                                if now >= expires_at {
                                                                    // Key has expired, remove it and return null
                                                                    store.remove(key);
                                                                    stream.write_all(b"$-1\r\n").unwrap();
                                                                } else {
                                                                    // Key hasn't expired, return the value
                                                                    let response = format!("${}\r\n{}\r\n", stored_value.value.len(), stored_value.value);
                                                                    stream.write_all(response.as_bytes()).unwrap();
                                                                }
                                                            } else {
                                                                // Key doesn't expire, return the value
                                                                let response = format!("${}\r\n{}\r\n", stored_value.value.len(), stored_value.value);
                                                                stream.write_all(response.as_bytes()).unwrap();
                                                            }
                                                        }
                                                        None => {
                                                            stream.write_all(b"$-1\r\n").unwrap();
                                                        }
                                                    }
                                                }
                                            }
                                            "CONFIG" => {
                                                if args.len() >= 3 && args[1].to_uppercase() == "GET" {
                                                    let param = &args[2];
                                                    match param.to_lowercase().as_str() {
                                                        "dir" => {
                                                            let response = format!("*2\r\n$3\r\ndir\r\n${}\r\n{}\r\n", 
                                                                config_clone.dir.len(), config_clone.dir);
                                                            stream.write_all(response.as_bytes()).unwrap();
                                                        }
                                                        "dbfilename" => {
                                                            let response = format!("*2\r\n$10\r\ndbfilename\r\n${}\r\n{}\r\n", 
                                                                config_clone.dbfilename.len(), config_clone.dbfilename);
                                                            stream.write_all(response.as_bytes()).unwrap();
                                                        }
                                                        _ => {
                                                            stream.write_all(b"*0\r\n").unwrap();
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
