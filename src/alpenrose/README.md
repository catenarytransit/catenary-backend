# Distributed compatible architecture

All worker architecture (leaderless)

Workers are responsible for reading the state of the password system and updating how often data is refreshed and what URLs to use.

Feeds currently taken are held via ephemeral znodes. If the node disconnects from zookeeper, it will be avaliable to for other workers to start using.

They send data to the Aspen datastore and processor over Grpc
Repeat sends to Aspen are prevented using a hash function for `gtfs_rt::FeedMessage`