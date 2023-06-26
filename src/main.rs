use std::net::TcpListener;
use std::thread::spawn;
use tungstenite::accept;

<<<<<<< HEAD
fn main () {
    let server = TcpListener::bind("127.0.0.1:9090").unwrap();
    for stream in server.incoming() {
        spawn (move || {
            let mut websocket = accept(stream.unwrap()).unwrap();
            loop {
                    if websocket.can_read() {
                        let msg = websocket.read_message().unwrap();

                        println!("{:?}",msg.to_string());

                // We do not want to send back ping/pong messages.
                if msg.is_binary() || msg.is_text() {
                    if websocket.can_write() {
                        websocket.write_message(msg).unwrap();
                    }
                    }
                    }
                
            }
        });
    }
}
=======
fn main() {
    println!("Hello, world!");
}
>>>>>>> be410575a4708c79411ca62b0dc9aee1d06118f8
