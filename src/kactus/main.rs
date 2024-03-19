use std::thread;
use std::time::Duration;

fn main() {
    // infinite runtime with worker threads and a leader-candidate thread that attempts to be the leader
    // Uses zookeeper to elect the leader and assign feeds to insert
    // Service discovery via zookeeper

    // if a node drops out, ingestion will be automatically reassigned to the other nodes

    //hands off data to aspen to do additional cleanup and processing, Aspen will perform association with the GTFS schedule data + update dynamic graphs for routing and map representation,
    //aspen will also forward critical alerts to users

    //spawn the thread to listen to be a leader
    thread::spawn(|| {

    });

    //worker thread
    thread::spawn(|| {
        
    })
}