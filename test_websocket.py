import asyncio
import websockets
import json

async def test_websocket(uri, chateau, trip_id, start_time=None, start_date=None, route_id=None):
    # Single endpoint 
    
    print(f"Connecting to {uri}...")
    try:
        async with websockets.connect(uri) as websocket:
            print(f"Connected to {uri}")

            # Send Subscribe Message with chateau
            subscribe_msg = {
                "type": "subscribe_trip",
                "chateau": chateau,
                "trip_id": trip_id,
            }
            
            if start_time:
                subscribe_msg["start_time"] = start_time
            if start_date:
                subscribe_msg["start_date"] = start_date
            if route_id:
                subscribe_msg["route_id"] = route_id
                
            await websocket.send(json.dumps(subscribe_msg))
            print(f"Sent: {subscribe_msg}")

            # Listen for messages
            while True:
                response = await websocket.recv()
                data = json.loads(response)
                print(f"Received: {data['type']}")
                
                if data['type'] == 'initial':
                    print("Initial data received (summary):")
                    print(f"  Trip Headsign: {data['data'].get('trip_headsign')}")
                    print(f"  Stoptimes: {len(data['data'].get('stoptimes', []))}")
                    # If we got initial data, test is successful enough for connection logic
                    break
                elif data['type'] == 'update':
                    print("Update received!")
                elif data['type'] == 'error':
                    print(f"Error: {data.get('message')}")
                    break
    except Exception as e:
        print(f"Connection failed: {e}")
        print("Ensure 'spruce' is running and env vars are set.")



if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Test WebSocket connection")
    parser.add_argument("--chateau", default="test_chateau", help="Chateau ID")
    parser.add_argument("--trip-id", default="test_trip_id", help="Trip ID")
    parser.add_argument("--uri", default="ws://localhost:52771/ws/", help="WebSocket URI")
    
    args = parser.parse_args()
    
    asyncio.get_event_loop().run_until_complete(test_websocket(args.uri, args.chateau, args.trip_id))
