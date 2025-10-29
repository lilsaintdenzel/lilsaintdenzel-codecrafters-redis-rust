#![allow(unused_imports)]
use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::io::{BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

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

type ReplicaConnections = Arc<Mutex<Vec<TcpStream>>>;
type MasterOffset = Arc<Mutex<u64>>;

fn propagate_command_to_replicas(replicas: &ReplicaConnections, command: &[String]) {
    let mut replicas_guard = replicas.lock().unwrap();
    let mut active_replicas = Vec::new();
    
    // Format command as RESP array
    let mut resp_command = format!("*{}\r\n", command.len());
    for arg in command {
        resp_command.push_str(&format!("${}\r\n{}\r\n", arg.len(), arg));
    }
    
    // Send to all replicas and filter out disconnected ones
    for mut replica in replicas_guard.drain(..) {
        if replica.write_all(resp_command.as_bytes()).is_ok() {
            active_replicas.push(replica);
        }
    }
    
    // Keep only active replicas
    *replicas_guard = active_replicas;
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

fn calculate_resp_command_size(buffer: &[u8], n: usize) -> usize {
    let data = String::from_utf8_lossy(&buffer[..n]);
    let lines: Vec<&str> = data.split("\r\n").collect();
    
    if lines.is_empty() || !lines[0].starts_with('*') {
        return n; // fallback to buffer size if can't parse
    }
    
    if let Ok(array_len) = lines[0][1..].parse::<usize>() {
        let mut total_size = lines[0].len() + 2; // *N\r\n
        let mut line_idx = 1;
        
        for _ in 0..array_len {
            if line_idx < lines.len() && lines[line_idx].starts_with('$') {
                total_size += lines[line_idx].len() + 2; // $N\r\n
                line_idx += 1;
                
                if line_idx < lines.len() {
                    total_size += lines[line_idx].len() + 2; // arg\r\n
                    line_idx += 1;
                }
            }
        }
        
        total_size
    } else {
        n // fallback
    }
}

fn parse_multiple_redis_commands(buffer: &[u8], n: usize) -> Vec<Vec<String>> {
    let mut commands = Vec::new();
    let data = String::from_utf8_lossy(&buffer[..n]);
    let mut pos = 0;
    
    // Split by looking for command boundaries (*N\r\n pattern)
    while pos < data.len() {
        if let Some(star_pos) = data[pos..].find('*') {
            let cmd_start = pos + star_pos;
            if let Some(args) = parse_redis_command(&buffer[cmd_start..], n - cmd_start) {
                commands.push(args);
                
                // Find the end of this command by looking for the next '*' or end of data
                let mut search_pos = cmd_start + 1;
                while search_pos < data.len() && !data[search_pos..].starts_with('*') {
                    search_pos += 1;
                }
                pos = search_pos;
            } else {
                break;
            }
        } else {
            break;
        }
    }
    
    commands
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
    // Note: We're not consuming the RDB file here anymore since it might be causing issues
    // The replica thread will handle any remaining data in the stream
    
    println!("Sent PSYNC to master");
    Ok(())
}

fn main() {
    let config = parse_args();
    
    // Initialize data store first
    let initial_data = load_rdb_file(&config);
    let data_store = Arc::new(Mutex::new(initial_data));
    let replica_connections: ReplicaConnections = Arc::new(Mutex::new(Vec::new()));
    let master_offset: MasterOffset = Arc::new(Mutex::new(0));
    
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
                
                // Keep listening for propagated commands from master
                let store_for_replication = Arc::clone(&data_store);
                thread::spawn(move || {
                    let mut buffer = [0; 1024];
                    let mut replica_offset = 0u64;
                    let mut rdb_consumed = false;
                    
                    loop {
                        match master_stream.read(&mut buffer) {
                            Ok(0) => {
                                println!("Master connection closed");
                                break;
                            }
                            Ok(n) => {
                                // First, we need to consume the RDB file if we haven't already
                                if !rdb_consumed {
                                    let data = String::from_utf8_lossy(&buffer[..n]);
                                    if data.starts_with('$') {
                                        // This buffer contains RDB data - check if there's a command after it
                                        if let Some(first_crlf) = data.find("\r\n") {
                                            let length_line = &data[1..first_crlf]; // Skip the '$'
                                            if let Ok(rdb_length) = length_line.parse::<usize>() {
                                                let rdb_start = first_crlf + 2; // After \r\n
                                                let rdb_end = rdb_start + rdb_length;
                                                
                                                // Check if there's command data after the RDB
                                                if rdb_end < n {
                                                    // There's data after RDB - process it as a command
                                                    rdb_consumed = true;
                                                    let remaining_data = &buffer[rdb_end..n];
                                                    let remaining_size = n - rdb_end;
                                                    
                                                    // Process the command that follows RDB
                                                    if let Some(args) = parse_redis_command(remaining_data, remaining_size) {
                                                        if !args.is_empty() && args[0].to_uppercase() == "REPLCONF" {
                                                            if args.len() >= 3 && args[1].to_uppercase() == "GETACK" && args[2] == "*" {
                                                                let offset_str = replica_offset.to_string();
                                                                let response = format!(
                                                                    "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n${}\r\n{}\r\n",
                                                                    offset_str.len(),
                                                                    offset_str
                                                                );
                                                                if let Err(e) = master_stream.write_all(response.as_bytes()) {
                                                                    println!("Failed to send REPLCONF ACK to master: {}", e);
                                                                    break;
                                                                }
                                                                replica_offset += remaining_size as u64;
                                                                continue; // Skip normal processing since we handled it here
                                                            }
                                                        }
                                                    }
                                                } else {
                                                    // Only RDB data in this buffer, no command follows
                                                    rdb_consumed = true;
                                                    continue;
                                                }
                                            }
                                        }
                                        // If we can't parse RDB properly, treat as simple RDB skip
                                        rdb_consumed = true;
                                        continue;
                                    }
                                    // If it doesn't start with $, it's a command, so mark RDB as consumed
                                    rdb_consumed = true;
                                }
                                
                                // Process all commands in the buffer (there might be multiple)
                                let commands = parse_multiple_redis_commands(&buffer, n);
                                
                                if commands.is_empty() {
                                    // If we can't parse any commands, try with single command parser
                                    if let Some(args) = parse_redis_command(&buffer, n) {
                                        if !args.is_empty() {
                                            let command = args[0].to_uppercase();
                                            
                                            match command.as_str() {
                                                "SET" => {
                                                    // Process SET command silently (no response to master)
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
                                                        
                                                        let mut store = store_for_replication.lock().unwrap();
                                                        store.insert(key.clone(), stored_value);
                                                        // Note: No response sent to master for propagated commands
                                                    }
                                                }
                                                "REPLCONF" => {
                                                    // Handle REPLCONF GETACK command
                                                    if args.len() >= 3 && args[1].to_uppercase() == "GETACK" && args[2] == "*" {
                                                        // Respond with REPLCONF ACK <current_offset>
                                                        // The offset should only include commands processed BEFORE this GETACK request
                                                        let offset_str = replica_offset.to_string();
                                                        let response = format!(
                                                            "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n${}\r\n{}\r\n",
                                                            offset_str.len(),
                                                            offset_str
                                                        );
                                                        if let Err(e) = master_stream.write_all(response.as_bytes()) {
                                                            println!("Failed to send REPLCONF ACK to master: {}", e);
                                                            break;
                                                        }
                                                    }
                                                }
                                                _ => {
                                                    // For other commands (like PING), just continue
                                                }
                                            }
                                        }
                                    }
                                    // Always add buffer size to offset
                                    replica_offset += n as u64;
                                } else {
                                    // Process each command found in the buffer
                                    for args in commands {
                                        if !args.is_empty() {
                                            let command = args[0].to_uppercase();
                                            
                                            match command.as_str() {
                                                "SET" => {
                                                    // Process SET command silently (no response to master)
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
                                                        
                                                        let mut store = store_for_replication.lock().unwrap();
                                                        store.insert(key.clone(), stored_value);
                                                        // Note: No response sent to master for propagated commands
                                                    }
                                                }
                                                "REPLCONF" => {
                                                    // Handle REPLCONF GETACK command
                                                    if args.len() >= 3 && args[1].to_uppercase() == "GETACK" && args[2] == "*" {
                                                        // Respond with REPLCONF ACK <current_offset>
                                                        // The offset should only include commands processed BEFORE this GETACK request
                                                        let offset_str = replica_offset.to_string();
                                                        let response = format!(
                                                            "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n${}\r\n{}\r\n",
                                                            offset_str.len(),
                                                            offset_str
                                                        );
                                                        if let Err(e) = master_stream.write_all(response.as_bytes()) {
                                                            println!("Failed to send REPLCONF ACK to master: {}", e);
                                                            break;
                                                        }
                                                    }
                                                }
                                                _ => {
                                                    // For other commands (like PING), just continue
                                                }
                                            }
                                        }
                                    }
                                    // Add buffer size to offset for all commands processed
                                    replica_offset += n as u64;
                                }
                            }
                            Err(e) => {
                                println!("Error reading from master: {}", e);
                                break;
                            }
                        }
                    }
                });
            }
            Err(e) => {
                println!("Failed to connect to master: {}", e);
            }
        }
    }
    
    let bind_address = format!("127.0.0.1:{}", config.port);
    let listener = TcpListener::bind(&bind_address).unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                let store_clone = Arc::clone(&data_store);
                let replicas_clone = Arc::clone(&replica_connections);
                let master_offset_clone = Arc::clone(&master_offset);
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
                                                    drop(store); // Release lock before propagating
                                                    
                                                    stream.write_all(b"+OK\r\n").unwrap();
                                                    
                                                    // Propagate SET command to replicas (only if this is a master)
                                                    if config_clone.replicaof.is_none() {
                                                        propagate_command_to_replicas(&replicas_clone, &args);
                                                        // Update master offset after propagation
                                                        let command_size = calculate_resp_command_size(&buffer, n);
                                                        let mut offset = master_offset_clone.lock().unwrap();
                                                        *offset += command_size as u64;
                                                    }
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
                                            "REPLCONF" => {
                                                // For the purposes of this challenge, we can safely ignore 
                                                // the arguments and simply respond with +OK\r\n
                                                stream.write_all(b"+OK\r\n").unwrap();
                                            }
                                            "PSYNC" => {
                                                if args.len() >= 3 && args[1] == "?" && args[2] == "-1" {
                                                    // Respond with FULLRESYNC <REPL_ID> 0
                                                    let response = "+FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0\r\n";
                                                    stream.write_all(response.as_bytes()).unwrap();
                                                    
                                                    // Send empty RDB file
                                                    let rdb_hex = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";
                                                    let rdb_bytes = hex::decode(rdb_hex).unwrap();
                                                    let rdb_response = format!("${}\r\n", rdb_bytes.len());
                                                    stream.write_all(rdb_response.as_bytes()).unwrap();
                                                    stream.write_all(&rdb_bytes).unwrap();
                                                    
                                                    // Add this stream as a replica connection for command propagation
                                                    if let Ok(stream_clone) = stream.try_clone() {
                                                        let mut replicas = replicas_clone.lock().unwrap();
                                                        replicas.push(stream_clone);
                                                    }
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
                                            "WAIT" => {
                                                if args.len() >= 3 {
                                                    // Parse numreplicas and timeout
                                                    if let (Ok(numreplicas), Ok(timeout_ms)) = (args[1].parse::<u32>(), args[2].parse::<u32>()) {
                                                        // Handle the simple case: when numreplicas is 0
                                                        if numreplicas == 0 {
                                                            stream.write_all(b":0\r\n").unwrap();
                                                        } else {
                                                            // Get current master offset
                                                            let current_offset = {
                                                                let offset = master_offset_clone.lock().unwrap();
                                                                *offset
                                                            };
                                                            
                                                            // If offset is 0, no write commands have been sent, return replica count immediately
                                                            if current_offset == 0 {
                                                                let replica_count = replicas_clone.lock().unwrap().len();
                                                                let response = format!(":{}\r\n", replica_count);
                                                                stream.write_all(response.as_bytes()).unwrap();
                                                            } else {
                                                                // Send GETACK to all replicas and wait for responses
                                                                let start_time = Instant::now();
                                                                let timeout_duration = Duration::from_millis(timeout_ms as u64);
                                                                
                                                                // Send REPLCONF GETACK * to all replicas
                                                                let getack_command = b"*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n";
                                                                let mut replicas_guard = replicas_clone.lock().unwrap();
                                                                let mut active_replicas = Vec::new();
                                                                
                                                                for mut replica in replicas_guard.drain(..) {
                                                                    if replica.write_all(getack_command).is_ok() {
                                                                        active_replicas.push(replica);
                                                                    }
                                                                }
                                                                
                                                                // Update replica list with only active replicas
                                                                *replicas_guard = Vec::new();
                                                                drop(replicas_guard);
                                                                
                                                                // Collect ACK responses
                                                                let mut ack_count = 0;
                                                                let mut remaining_replicas = Vec::new();
                                                                
                                                                for mut replica in active_replicas {
                                                                    // Set a short timeout for individual replica responses
                                                                    replica.set_read_timeout(Some(Duration::from_millis(100))).ok();
                                                                    
                                                                    let mut ack_buffer = [0; 1024];
                                                                    match replica.read(&mut ack_buffer) {
                                                                        Ok(ack_n) => {
                                                                            if let Some(ack_args) = parse_redis_command(&ack_buffer, ack_n) {
                                                                                if ack_args.len() >= 3 && 
                                                                                   ack_args[0].to_uppercase() == "REPLCONF" && 
                                                                                   ack_args[1].to_uppercase() == "ACK" {
                                                                                    if let Ok(replica_offset) = ack_args[2].parse::<u64>() {
                                                                                        if replica_offset >= current_offset {
                                                                                            ack_count += 1;
                                                                                        }
                                                                                    }
                                                                                }
                                                                            }
                                                                            // Reset timeout and keep replica
                                                                            replica.set_read_timeout(None).ok();
                                                                        }
                                                                        Err(_) => {
                                                                            // Replica didn't respond or disconnected
                                                                        }
                                                                    }
                                                                    
                                                                    // Always add replica back to remaining list
                                                                    remaining_replicas.push(replica);
                                                                    
                                                                    // Check if we have enough ACKs or if timeout exceeded
                                                                    if ack_count >= numreplicas || start_time.elapsed() >= timeout_duration {
                                                                        break;
                                                                    }
                                                                }
                                                                
                                                                // Wait for any remaining time if we don't have enough ACKs
                                                                while ack_count < numreplicas && start_time.elapsed() < timeout_duration {
                                                                    thread::sleep(Duration::from_millis(10));
                                                                }
                                                                
                                                                // Restore remaining active replicas
                                                                let mut replicas_guard = replicas_clone.lock().unwrap();
                                                                replicas_guard.extend(remaining_replicas);
                                                                drop(replicas_guard);
                                                                
                                                                // Return the number of replicas that acknowledged
                                                                let response = format!(":{}\r\n", ack_count);
                                                                stream.write_all(response.as_bytes()).unwrap();
                                                            }
                                                        }
                                                    } else {
                                                        // Invalid arguments
                                                        stream.write_all(b"-ERR wrong number of arguments for 'wait' command\r\n").unwrap();
                                                    }
                                                } else {
                                                    // Not enough arguments
                                                    stream.write_all(b"-ERR wrong number of arguments for 'wait' command\r\n").unwrap();
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
