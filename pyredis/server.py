import socket
import asyncio
from protocol import parse_frame

PORT = 6379 # Redis Port
BUFFER_SIZE = 4096
ADDRESS = 'localhost'

async def handle_connection(client):
    loop = asyncio.get_running_loop()
    frame_buffer = bytearray()
    while True:
        try:
            msg = await loop.sock_recv(client, BUFFER_SIZE)
            frame_buffer.extend(msg)
            while len(frame_buffer) > 0:
                frame, size = parse_frame(frame_buffer)
                if frame is None:
                    break
                else:
                    frame_buffer = frame_buffer[size:]
                    await loop.sock_sendall(client, frame.serialize())
        except (ConnectionResetError, asyncio.CancelledError):
            print('Client disconnected or server shutdown')
            return


async def server():
    with socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM) as s:
        s.bind((ADDRESS, PORT))
        s.listen()
        s.setblocking(False)
        loop = asyncio.get_running_loop()
        conns = []

        print(f'Server listening on {ADDRESS}:{PORT}...')
        while True:
            try:
                client, address =  await loop.sock_accept(s)
                print(f"Handling connection from {address}")
                conns.append(asyncio.create_task(handle_connection(client)))

            except (KeyboardInterrupt, asyncio.CancelledError):
                print(f'Shutting Down')
                for c in conns:
                    c.cancel()

                if conns:
                    await asyncio.gather(*conns, return_exceptions=True)
                raise


if __name__ == '__main__':
    try:
        asyncio.run(server())
    except KeyboardInterrupt:
        print('Shutdown...')



