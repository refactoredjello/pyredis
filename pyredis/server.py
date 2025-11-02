import asyncio
import socket
import traceback

from pyredis.commands import Command
from pyredis.expiry import run_cleanup_in_background
from pyredis.persist import AOF
from pyredis.protocol import Error, parse_frame
from pyredis.store import DataStoreWithLock, DataStoreWithQueue

PORT = 6379  # Redis Port
BUFFER_SIZE = 4096
HOST = "localhost"
AOF_NAME = "dump.aof"


async def handle_connection(client, datastore, buffer_size, cmd_logger):
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
                    del frame_buffer[:size]
                    try:
                        response = await Command(frame, datastore, cmd_logger).exec()
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
    datastore = DataStoreWithLock()
    cmd_logger = AOF(AOF_NAME)

    datastore_worker = datastore.start()
    cull_worker = asyncio.create_task(run_cleanup_in_background(datastore))
    cmd_logger_worker = asyncio.create_task(cmd_logger.run_worker())
    loop = asyncio.get_running_loop()
    conns = []

    with socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((host, port))
        s.listen()
        s.setblocking(False)

        print(f"Server Listening: {host}:{port}...")
        while True:
            try:
                client, address = await loop.sock_accept(s)
                # print(f"Handling connection from {address}")
                conns.append(
                    asyncio.create_task(
                        handle_connection(client, datastore, buffer_size, cmd_logger)
                    )
                )

            except (KeyboardInterrupt, SystemExit, asyncio.CancelledError):
                print(f"Shutting Down")
                for c in conns:
                    c.cancel()

                datastore_worker.cancel()
                cull_worker.cancel()

                if conns:
                    await asyncio.gather(*conns, return_exceptions=True)
                await asyncio.gather(
                    datastore_worker,
                    cull_worker,
                    cmd_logger_worker,
                    return_exceptions=True,
                )
                raise
