#![allow(unused_imports)]
use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::io::{BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::Path;
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
    port: u16,
    replicaof: Option<(String, u16)>, // (host, port) if this is a replica
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

fn parse_length_encoding(bytes: &[u8], pos: &mut usize) -> Option<u64> {
    if *pos >= bytes.len() {
        return None;
    }
    
    let first_byte = bytes[*pos];
    *pos += 1;
    
    let first_two_bits = (first_byte & 0b11000000) >> 6;
    
    match first_two_bits {
        0b00 => {
            // Length is the remaining 6 bits
            Some((first_byte & 0b00111111) as u64)
        }
        0b01 => {
            // Length is next 14 bits (6 + 8)
            if *pos >= bytes.len() {
                return None;
            }
            let second_byte = bytes[*pos];
            *pos += 1;
            let length = (((first_byte & 0b00111111) as u64) << 8) | (second_byte as u64);
            Some(length)
        }
        0b10 => {
            // Length is next 4 bytes in big-endian
            if *pos + 4 > bytes.len() {
                return None;
            }
            let mut length = 0u64;
            for i in 0..4 {
                length = (length << 8) | (bytes[*pos + i] as u64);
            }
            *pos += 4;
            Some(length)
        }
        0b11 => {
            // Special string encoding - return the remaining 6 bits as a special marker
            Some(0xFF00 | ((first_byte & 0b00111111) as u64))
        }
        _ => None,
    }
}

fn parse_string_encoding(bytes: &[u8], pos: &mut usize) -> Option<String> {
    let length_or_type = parse_length_encoding(bytes, pos)?;
    
    if length_or_type >= 0xFF00 {
        // Special string encoding
        let encoding_type = (length_or_type & 0xFF) as u8;
        match encoding_type {
            0x00 => {
                // 8-bit integer
                if *pos >= bytes.len() {
                    return None;
                }
                let val = bytes[*pos] as i8;
                *pos += 1;
                Some(val.to_string())
            }
            0x01 => {
                // 16-bit integer (little-endian)
                if *pos + 2 > bytes.len() {
                    return None;
                }
                let val = u16::from_le_bytes([bytes[*pos], bytes[*pos + 1]]) as i16;
                *pos += 2;
                Some(val.to_string())
            }
            0x02 => {
                // 32-bit integer (little-endian)
                if *pos + 4 > bytes.len() {
                    return None;
                }
                let val = u32::from_le_bytes([
                    bytes[*pos],
                    bytes[*pos + 1],
                    bytes[*pos + 2],
                    bytes[*pos + 3],
                ]) as i32;
                *pos += 4;
                Some(val.to_string())
            }
            _ => None,
        }
    } else {
        // Regular string
        let len = length_or_type as usize;
        if *pos + len > bytes.len() {
            return None;
        }
        let string_bytes = &bytes[*pos..*pos + len];
        *pos += len;
        String::from_utf8(string_bytes.to_vec()).ok()
    }
}

fn matches_pattern(key: &str, pattern: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    
    // Simple glob pattern matching
    // For now, only support "*" (match all) and exact matches
    if pattern.contains('*') {
        if pattern == "*" {
            return true;
        }
        // More complex pattern matching would go here
        // For this stage, we mainly need to support "*"
        true
    } else {
        key == pattern
    }
}

fn load_rdb_file(config: &Config) -> HashMap<String, StoredValue> {
    let mut data_store = HashMap::new();
    let rdb_path = Path::new(&config.dir).join(&config.dbfilename);
    
    if !rdb_path.exists() {
        return data_store;
    }
    
    let file = match File::open(&rdb_path) {
        Ok(f) => f,
        Err(_) => return data_store,
    };
    
    let mut reader = BufReader::new(file);
    let mut buffer = Vec::new();
    if reader.read_to_end(&mut buffer).is_err() {
        return data_store;
    }
    
    let mut pos = 0;
    
    // Parse header ("REDIS0011")
    if buffer.len() < 9 || &buffer[0..5] != b"REDIS" {
        return data_store;
    }
    pos = 9;
    
    while pos < buffer.len() {
        let opcode = buffer[pos];
        pos += 1;
        
        match opcode {
            0xFA => {
                // Metadata section - skip
                if let Some(_key) = parse_string_encoding(&buffer, &mut pos) {
                    let _value = parse_string_encoding(&buffer, &mut pos);
                }
            }
            0xFE => {
                // Database section
                let _db_index = parse_length_encoding(&buffer, &mut pos);
            }
            0xFB => {
                // Hash table size info - skip
                let _hash_table_size = parse_length_encoding(&buffer, &mut pos);
                let _expire_hash_table_size = parse_length_encoding(&buffer, &mut pos);
            }
            0xFC => {
                // Key with millisecond expiry
                if pos + 8 > buffer.len() {
                    break;
                }
                let expire_timestamp = u64::from_le_bytes([
                    buffer[pos],
                    buffer[pos + 1],
                    buffer[pos + 2],
                    buffer[pos + 3],
                    buffer[pos + 4],
                    buffer[pos + 5],
                    buffer[pos + 6],
                    buffer[pos + 7],
                ]) as u128;
                pos += 8;
                
                // Value type (should be 0 for string)
                if pos >= buffer.len() {
                    break;
                }
                let _value_type = buffer[pos];
                pos += 1;
                
                // Key and value
                if let Some(key) = parse_string_encoding(&buffer, &mut pos) {
                    if let Some(value) = parse_string_encoding(&buffer, &mut pos) {
                        data_store.insert(
                            key,
                            StoredValue {
                                value,
                                expires_at: Some(expire_timestamp),
                            },
                        );
                    }
                }
            }
            0xFD => {
                // Key with second expiry
                if pos + 4 > buffer.len() {
                    break;
                }
                let expire_timestamp = u32::from_le_bytes([
                    buffer[pos],
                    buffer[pos + 1],
                    buffer[pos + 2],
                    buffer[pos + 3],
                ]) as u128
                    * 1000; // Convert to milliseconds
                pos += 4;
                
                // Value type (should be 0 for string)
                if pos >= buffer.len() {
                    break;
                }
                let _value_type = buffer[pos];
                pos += 1;
                
                // Key and value
                if let Some(key) = parse_string_encoding(&buffer, &mut pos) {
                    if let Some(value) = parse_string_encoding(&buffer, &mut pos) {
                        data_store.insert(
                            key,
                            StoredValue {
                                value,
                                expires_at: Some(expire_timestamp),
                            },
                        );
                    }
                }
            }
            0xFF => {
                // End of file
                break;
            }
            _ => {
                // Regular key-value pair (value type)
                let _value_type = opcode;
                
                // Key and value
                if let Some(key) = parse_string_encoding(&buffer, &mut pos) {
                    if let Some(value) = parse_string_encoding(&buffer, &mut pos) {
                        data_store.insert(
                            key,
                            StoredValue {
                                value,
                                expires_at: None,
                            },
                        );
                    }
                }
            }
        }
    }
    
    data_store
}

fn parse_args() -> Config {
    let args: Vec<String> = env::args().collect();
    let mut dir = "/tmp/redis-data".to_string();
    let mut dbfilename = "dump.rdb".to_string();
    let mut port = 6379u16;
    let mut replicaof = None;
    
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
            "--port" => {
                if i + 1 < args.len() {
                    if let Ok(parsed_port) = args[i + 1].parse::<u16>() {
                        port = parsed_port;
                    }
                    i += 2;
                } else {
                    i += 1;
                }
            }
            "--replicaof" => {
                if i + 1 < args.len() {
                    let master_info = &args[i + 1];
                    let parts: Vec<&str> = master_info.split_whitespace().collect();
                    if parts.len() == 2 {
                        if let Ok(master_port) = parts[1].parse::<u16>() {
                            replicaof = Some((parts[0].to_string(), master_port));
                        }
                    }
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
    
    Config { dir, dbfilename, port, replicaof }
}

fn connect_to_master(master_host: &str, master_port: u16) -> Result<TcpStream, std::io::Error> {
    let master_address = format!("{}:{}", master_host, master_port);
    println!("Connecting to master at {}", master_address);
    TcpStream::connect(master_address)
}

fn send_ping_to_master(stream: &mut TcpStream) -> Result<(), std::io::Error> {
    // Send PING command as RESP array: *1\r\n$4\r\nPING\r\n
    let ping_command = b"*1\r\n$4\r\nPING\r\n";
    stream.write_all(ping_command)?;
    
    // Read response (should be +PONG\r\n)
    let mut buffer = [0; 1024];
    let _n = stream.read(&mut buffer)?;
    // We don't need to process the response for this stage
    
    println!("Sent PING to master");
    Ok(())
}

fn send_replconf_listening_port(stream: &mut TcpStream, port: u16) -> Result<(), std::io::Error> {
    // Send REPLCONF listening-port <PORT> as RESP array
    // *3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n6380\r\n
    let port_str = port.to_string();
    let command = format!(
        "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n${}\r\n{}\r\n",
        port_str.len(),
        port_str
    );
    stream.write_all(command.as_bytes())?;
    
    // Read response (should be +OK\r\n)
    let mut buffer = [0; 1024];
    let _n = stream.read(&mut buffer)?;
    
    println!("Sent REPLCONF listening-port to master");
    Ok(())
}

fn send_replconf_capa_psync2(stream: &mut TcpStream) -> Result<(), std::io::Error> {
    // Send REPLCONF capa psync2 as RESP array
    // *3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n
    let command = b"*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n";
    stream.write_all(command)?;
    
    // Read response (should be +OK\r\n)
    let mut buffer = [0; 1024];
    let _n = stream.read(&mut buffer)?;
    
    println!("Sent REPLCONF capa psync2 to master");
    Ok(())
}

fn send_psync_to_master(stream: &mut TcpStream) -> Result<(), std::io::Error> {
    // Send PSYNC ? -1 as RESP array
    // *3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n
    let command = b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n";
    stream.write_all(command)?;
    
    // Read response (should be +FULLRESYNC <REPL_ID> 0\r\n)
    let mut buffer = [0; 1024];
    let _n = stream.read(&mut buffer)?;
    // We can ignore the response for now as per the requirements
    
    println!("Sent PSYNC to master");
    Ok(())
}

fn main() {
    let config = parse_args();
    
    // If this is a replica, connect to master and perform handshake
    if let Some((master_host, master_port)) = &config.replicaof {
        match connect_to_master(master_host, *master_port) {
            Ok(mut master_stream) => {
                // Send PING
                if let Err(e) = send_ping_to_master(&mut master_stream) {
                    println!("Failed to send PING to master: {}", e);
                    return;
                }
                
                // Send REPLCONF listening-port
                if let Err(e) = send_replconf_listening_port(&mut master_stream, config.port) {
                    println!("Failed to send REPLCONF listening-port to master: {}", e);
                    return;
                }
                
                // Send REPLCONF capa psync2
                if let Err(e) = send_replconf_capa_psync2(&mut master_stream) {
                    println!("Failed to send REPLCONF capa psync2 to master: {}", e);
                    return;
                }
                
                // Send PSYNC
                if let Err(e) = send_psync_to_master(&mut master_stream) {
                    println!("Failed to send PSYNC to master: {}", e);
                    return;
                }
            }
            Err(e) => {
                println!("Failed to connect to master: {}", e);
            }
        }
    }
    
    let bind_address = format!("127.0.0.1:{}", config.port);
    let listener = TcpListener::bind(&bind_address).unwrap();
    let initial_data = load_rdb_file(&config);
    let data_store = Arc::new(Mutex::new(initial_data));

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
                                            "KEYS" => {
                                                if args.len() >= 2 {
                                                    let pattern = &args[1];
                                                    let store = store_clone.lock().unwrap();
                                                    let now = SystemTime::now()
                                                        .duration_since(UNIX_EPOCH)
                                                        .unwrap()
                                                        .as_millis();
                                                    
                                                    let mut matching_keys = Vec::new();
                                                    
                                                    for (key, stored_value) in store.iter() {
                                                        // Check if key has expired
                                                        let is_expired = if let Some(expires_at) = stored_value.expires_at {
                                                            now >= expires_at
                                                        } else {
                                                            false
                                                        };
                                                        
                                                        if !is_expired && matches_pattern(key, pattern) {
                                                            matching_keys.push(key.clone());
                                                        }
                                                    }
                                                    
                                                    let mut response = format!("*{}\r\n", matching_keys.len());
                                                    for key in matching_keys {
                                                        response.push_str(&format!("${}\r\n{}\r\n", key.len(), key));
                                                    }
                                                    stream.write_all(response.as_bytes()).unwrap();
                                                } else {
                                                    stream.write_all(b"*0\r\n").unwrap();
                                                }
                                            }
                                            "INFO" => {
                                                if args.len() >= 2 && args[1].to_lowercase() == "replication" {
                                                    let role = if config_clone.replicaof.is_some() {
                                                        "slave"
                                                    } else {
                                                        "master"
                                                    };
                                                    let mut info_lines = vec![format!("role:{}", role)];
                                                    
                                                    if config_clone.replicaof.is_none() {
                                                        info_lines.push("master_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string());
                                                        info_lines.push("master_repl_offset:0".to_string());
                                                    }
                                                    
                                                    let info_response = info_lines.join("\r\n");
                                                    let response = format!("${}\r\n{}\r\n", info_response.len(), info_response);
                                                    stream.write_all(response.as_bytes()).unwrap();
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
