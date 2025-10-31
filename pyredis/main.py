from pyredis.server import server
import asyncio


def main():
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


if __name__ == "__main__":
    main()
