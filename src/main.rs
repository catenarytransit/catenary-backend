use std::net::TcpListener;
use std::thread::spawn;
use tungstenite::accept;

<<<<<<< HEAD
fn main () {
=======
fn main() {
>>>>>>> c29cf9165b7917f2053328b4cc1461322ad3e66d
    let server = TcpListener::bind("127.0.0.1:9090").unwrap();
    for stream in server.incoming() {
        spawn(move || {
            let mut websocket = accept(stream.unwrap()).unwrap();
            loop {
                if websocket.can_read() {
                    let msg = websocket.read_message().unwrap();

                    println!("{:?}", msg.to_string());

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
