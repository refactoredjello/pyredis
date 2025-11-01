import socket
import asyncio
import traceback

from pyredis.protocol import parse_frame, Error
from pyredis.commands import Command
from pyredis.store import DataStore

PORT = 6379  # Redis Port
BUFFER_SIZE = 4096
HOST = "localhost"


async def handle_connection(client, datastore, buffer_size):
    loop = asyncio.get_running_loop()
    frame_buffer = bytearray()
    try:
        while True:
            msg = await loop.sock_recv(client, buffer_size)
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
                            print("Resp Err: ", response.decode())
                    except Exception:
                        print("Unhandled error", traceback.format_exc())
                        error = Error(f"Server error".encode())
                        await loop.sock_sendall(client, error.serialize())
                else:
                    break
    except ConnectionResetError:
        pass
    except asyncio.CancelledError:
        raise
    except Exception as e:
        print(f"Error handling connection: {e}")
    finally:
        client.close()


async def server(host=HOST, port=PORT, buffer_size=BUFFER_SIZE):
    datastore = DataStore()
    loop = asyncio.get_running_loop()
    worker_task = asyncio.create_task(datastore.run_worker())
    conns = []

    with socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((host, port))
        s.listen()
        s.setblocking(False)

        print(f"Server listening on {host}:{port}...")
        while True:
            try:
                client, address = await loop.sock_accept(s)
                # print(f"Handling connection from {address}")
                conns.append(asyncio.create_task(handle_connection(client, datastore, buffer_size)))

            except (KeyboardInterrupt, SystemExit, asyncio.CancelledError):
                print(f"Shutting Down")
                for c in conns:
                    c.cancel()

                worker_task.cancel()

                if conns:
                    await asyncio.gather(*conns, return_exceptions=True)
                await asyncio.gather(worker_task, return_exceptions=True)
                raise
