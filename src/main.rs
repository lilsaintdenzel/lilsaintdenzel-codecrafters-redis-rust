#![allow(unused_imports)]
use std::io::Write; 
use std::net::TcpListener;

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    // println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    
    for stream in listener.incoming() {
        match stream {
            // Ok(_stream) => {
            //     println!("accepted new connection");
            // }

            Ok(mut stream) => {

                let mut buffer = [0, 1024];

                loop {
                    match stream.read(&mut buffer) {
                       Ok(0) => break,
                       Ok(_) => {
                        stream.write_all(b"+PONG\r\n").unwrap();
                        
                       }
                          Err(e) => {
                            println!("error reading from stream: {}", e);
                            break;
                          }


                    }
                  
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
