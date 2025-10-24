import socket
import asyncio

PORT = 6379 # Redis Port
BUFFER_SIZE = 4096
ADDRESS = 'localhost'


async def handle_connection(client):
    loop = asyncio.get_running_loop()
    while True:
        try:
            msg = await loop.sock_recv(client, BUFFER_SIZE)
            await loop.sock_sendall(client, msg)
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

        print(f'Server listening on {ADDRESS}:{PORT}')
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
        print('Shutdown')



