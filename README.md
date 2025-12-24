# PyRedis

PyRedis is a from-scratch implementation of a Redis server in Python 3.14. The project is purely educational, designed to explore network programming, the Redis Serialization Protocol (RESP), and asynchronous I/O in Python.

## Features

- **Asynchronous Architecture**: Built on top of Python's `asyncio` to handle concurrent client connections efficiently.
- **RESP Protocol**: Full implementation of the Redis Serialization Protocol for parsing and serialization.
- **Persistence**: Supports Append-Only File (AOF) logging to persist data across restarts.
- **Expiration**: Active key expiration mechanism running in the background.
- **Data-Store Concurrency Models**:
  - **Queue-based (Default)**: Uses an actor-like pattern with `asyncio.Queue` for thread-safe state management. This is similar to the redis implementation. 
  - **Lock-based**: Alternative implementation using `asyncio.Lock` available via configuration. 

## Supported Commands

PyRedis currently supports a subset of Redis commands:

- **Keys**: `DEL`, `EXISTS`, `DBSIZE`, `INFO`
- **Strings**:
  - `GET`, `SET` (supports `EX`, `PX`, `EXAT`, `PXAT`, `NX`, `XX`, `GET` arguments)
  - `INCR`, `DECR`
- **Lists**: `LPUSH`, `RPUSH`, `LRANGE`
- **Connection**: `PING`, `ECHO`

## Configuration

The server can be configured via command-line arguments:

| Argument | Description | Default |
|----------|-------------|---------|
| `-a, --address` | Host address to bind to | `localhost` |
| `-p, --port` | Port number | `6379` |
| `-b, --buffer_size` | Network buffer size | `4096` |
| `-e, --expiry_interval` | Interval (seconds) for background expiry task | `300` |
| `-f, --cmd_log_name` | Name of the AOF log file | `dump.aof` |
| `-l, --load` | Load data from AOF file on startup | `False` |
| `-dsl, --lock` | Use lock-based datastore instead of queue-based | `False` |

## Setup

The app uses [mise-en-place](https://mise.jdx.dev/) for tool version management and task running.

1. Setup mise: https://mise.jdx.dev/
2. Run the setup task:
```bash
mise setup
```

## Running the Server

Start the server using mise:
```bash
mise serve
```

Start in development (watch) mode:
```bash
mise dev
```

Or run directly with python to pass arguments (e.g., to load persistence):
```bash
python -m pyredis.main --load
```

## Testing

1. **Basic Connectivity**:
   Install `redis-cli` and run:
   ```bash
   redis-cli -p 6379 PING
   # Expected: PONG
   ```

2. **Expiry Test**:
   Create 1000 keys with 10s expiry and observe the database size dropping:
   ```bash
   for i in {1..1000}; do (redis-cli set $i $i EX 10); done
   redis-cli DBSIZE
   ```

3. **Benchmark**:
   Run the standard redis benchmarking tool:
   ```bash
   mise benchmark
   ```
