import asyncio
import websockets
import json
import time
async def test_websocket_client(n_games):
    uri = f"ws://127.0.0.1:8001/ws/collect_fens/{n_games}"
    try:
        async with websockets.connect(uri) as websocket:
            print(f"Connected to {uri}")
            while True:
                try:
                    message = await websocket.recv()
                    print(f"Received: {message}")
                    if "BYEEEE" in message:
                        print('EVERYTHING IS DONE NOW')
                        time.sleep(2)
                        break
                        
                except websockets.exceptions.ConnectionClosedOK:
                    print("Connection closed normally.")
                    break
                except websockets.exceptions.ConnectionClosedError as e:
                    print(f"Connection closed with error: {e}")
                    break
                except Exception as e:
                    print(f"An unexpected error occurred: {e}")
                    break

    except Exception as e:
        print(f"Could not connect: {e}")

if __name__ == "__main__":
    asyncio.run(test_websocket_client(n_games=200)) # Adjust n_games as needed