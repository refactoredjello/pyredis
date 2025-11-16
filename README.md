**PyRedis**

PyRedis is a from-scratch implementation of a Redis server in python. The motivation is purely educational.
The goal is to get a better understanding of network programming via a full implementation of RESP(Redis Serialization Protocol), 
and a lower level understanding of high performance in-memory data structures and stores in single threaded model. 

Currently, supports GET, SET, ECHO, PING, INCR, DECR, LPUSH, RPUSH, LRANGE, DBSIZE commands.

**Setup**

The app uses mise-en-place for tool version management and dev task runners.  
1. Setup mise -> https://mise.jdx.dev/
2. Run the setup task runner. 
```bash
mise setup
```
**Run the server**

Start the server:
```bash
mise serve
```

Start in watch mode:
```bash
mise dev
```

**Test the server**

 - Install the redis-cli and run `redis-cli PING`. You should get a response `PONG`. 
 - Create 1000 keys `for i in {1..1000}; do (redis-cli set $i $i EX 10); done` and observe they are expired over time with `redis-cli DBSZIE`
 - Run the redis benchmarking tool with `mise benchmark`
