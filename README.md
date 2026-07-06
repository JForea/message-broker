# Message Broker

IPC message broker using Unix Domain Sockets.

## Configure

```bash
cmake -S . -B build
```

## Build

```bash
cmake --build build
```

## Run tests

```bash
ctest --test-dir build --output-on-failure
```