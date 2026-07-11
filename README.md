# Message Broker

IPC message broker using Unix Domain Sockets.

## Configure

```bash
cmake -S . -B build
```

Or if you would like to build example applications too:

```bash
cmake -S . -B build -DBUILD_EXAMPLES=ON
```

## Build

```bash
cmake --build build
```

## Run tests

```bash
ctest --test-dir build --output-on-failure
```