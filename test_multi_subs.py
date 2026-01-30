import asyncio
import websockets
import json
import argparse

async def test_multi_subs(uri, chateau, trip_id_1, trip_id_2):
    print(f"Connecting to {uri}...")
    try:
        async with websockets.connect(uri) as websocket:
            print(f"Connected to {uri}")

            # Subscribe to Trip 1
            sub1 = {
                "type": "subscribe_trip",
                "chateau": chateau,
                "trip_id": trip_id_1,
            }
            await websocket.send(json.dumps(sub1))
            print(f"Sent Sub 1: {sub1}")

            # Subscribe to Trip 2
            sub2 = {
                "type": "subscribe_trip",
                "chateau": chateau,
                "trip_id": trip_id_2,
            }
            await websocket.send(json.dumps(sub2))
            print(f"Sent Sub 2: {sub2}")

            count = 0
            while count < 5:
                response = await websocket.recv()
                data = json.loads(response)
                msg_type = data.get('type')
                
                if msg_type == 'initial_trip':
                     trip_id = data.get('data', {}).get('trip_id')
                     print(f"Received Initial Trip Data for: {trip_id}")
                elif msg_type == 'update_trip':
                     trip_id = data.get('data', {}).get('trip_id')
                     print(f"Received Update for: {trip_id}")
                elif msg_type == 'error':
                     print(f"Error: {data.get('message')}")

                count += 1
                
            # Unsubscribe from Trip 1
            unsub1 = {
                "type": "unsubscribe_trip",
                "chateau": chateau,
                "trip_id": trip_id_1,
            }
            await websocket.send(json.dumps(unsub1))
            print(f"Sent Unsub 1: {unsub1}")
            
            # Wait a bit to see if we get updates only for Trip 2
            await asyncio.sleep(2)
            
            # Unsubscribe all
            unsub_all = {
                "type": "unsubscribe_all_trips"
            }
            await websocket.send(json.dumps(unsub_all))
            print("Sent Unsub All")
            
    except Exception as e:
        print(f"Connection failed: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Test Multi-Trip Subscription")
    parser.add_argument("--chateau", default="test_chateau", help="Chateau ID")
    parser.add_argument("--trip-id-1", default="trip_1", help="Trip ID 1")
    parser.add_argument("--trip-id-2", default="trip_2", help="Trip ID 2")
    parser.add_argument("--uri", default="ws://localhost:52771/ws/", help="WebSocket URI")
    
    args = parser.parse_args()
    
    asyncio.get_event_loop().run_until_complete(test_multi_subs(args.uri, args.chateau, args.trip_id_1, args.trip_id_2))
