from pyredis.server import server, PORT, BUFFER_SIZE, HOST
import argparse
import asyncio


def main():
    parser = argparse.ArgumentParser(
        description="PyRedis, a basic implementation of redis in python that conforms to the"
        "the RESP protocol."
    )
    parser.add_argument(
        "-a","--address",
        type=str,
        help="The address to listen for requests, defaults to a local interface.",
        default=HOST,
        required=False,
    )
    parser.add_argument(
        "-p", "--port",
        type=int,
        help="The server port defaults to 6379",
        default=PORT,
        required=False,
    )
    parser.add_argument(
        "-b", "--buffer_size",
        type=int,
        help="The buffer size for messages",
        default=BUFFER_SIZE,
        required=False,
    )

    args = parser.parse_args()
    try:
        asyncio.run(server(args.address, args.port, args.buffer_size))
    except KeyboardInterrupt:
        print("Shutdown...")
    except Exception as e:
        print(f"Server error: {e}")
    finally:
        # Give OS time to release the port
        import time

        time.sleep(0.1)


if __name__ == "__main__":
    main()
