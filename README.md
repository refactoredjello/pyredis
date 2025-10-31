**PyRedis**

PyRedis is a from-scratch implementation of a Redis server in python. The motivation is purely educational.
The goal is to get a better understanding of networking programming via a full implementation of the RESP protocol, 
and a lower level understanding of in-memory data structures and stores.

Currently, supports GET, SET, ECHO, PING, INCR, DECR commands.

**Setup**

The app uses mise-en-place for runtime version management and development task runners.  
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

Install the redis-cli and run `redis-cli PING`. You should get a response `PONG`. 
