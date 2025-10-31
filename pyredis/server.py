import socket
import asyncio
from pyredis.protocol import parse_frame, Error
from pyredis.commands import Command
from pyredis.store import DataStore

PORT = 6379  # Redis Port
BUFFER_SIZE = 4096
ADDRESS = "localhost"


async def handle_connection(client, datastore):
    loop = asyncio.get_running_loop()
    frame_buffer = bytearray()
    try:
        while True:
            msg = await loop.sock_recv(client, BUFFER_SIZE)
            if not msg:
                break

            frame_buffer.extend(msg)
            while len(frame_buffer) > 0:
                frame, size = parse_frame(frame_buffer)
                if frame is not None:
                    frame_buffer = frame_buffer[size:]
                    try:
                        response = await Command(frame, datastore).exec()
                        await loop.sock_sendall(client, response.serialize())
                        if isinstance(response, Error):
                            print('Resp Err: ', response.decode())
                        else:
                            print('Resp: OK')
                    except Exception as ue:
                        error = Error(f'Unhandled error: {ue}'.encode())
                        print('Unhandled error', ue)
                        await loop.sock_sendall(client, error.serialize())
                        raise
                else:
                    break
    except (ConnectionResetError, asyncio.CancelledError):
        pass
    except Exception as e:
        print(f"Error handling connection: {e}")
    finally:
        client.close()


async def server():
    datastore = DataStore()
    loop = asyncio.get_running_loop()
    worker_task = asyncio.create_task(datastore.run_worker())
    conns = []

    with socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM) as s:
        s.bind((ADDRESS, PORT))
        s.listen()
        s.setblocking(False)

        print(f"Server listening on {ADDRESS}:{PORT}...")
        while True:
            try:
                client, address = await loop.sock_accept(s)
                print(f"Handling connection from {address}")
                conns.append(asyncio.create_task(handle_connection(client, datastore)))

            except (KeyboardInterrupt, SystemExit, asyncio.CancelledError):
                print(f"Shutting Down")
                for c in conns:
                    c.cancel()

                worker_task.cancel()

                if conns:
                    await asyncio.gather(*conns, return_exceptions=True)
                await asyncio.gather(worker_task, return_exceptions=True)
                raise


if __name__ == "__main__":
    try:
        asyncio.run(server())
    except KeyboardInterrupt:
        print("Shutdown...")
    except Exception as e:
        print(f"Server error: {e}")
    finally:
        # Give OS time to release the port
        import time
        time.sleep(0.1)
