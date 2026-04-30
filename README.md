# my-redis
## Intro
`my-redis` is a Redis clone implemented in Rust using the [Tokio](https://tokio.rs/) asynchronous runtime.

## Features

- RESP parser
- SET with optional expiry
- Lists
- Streams
- Transactions
- Replication

## Usage

```shell
# Run a primary instance on default port 6379 with info level logging
RUST_LOG=info cargo run

# Run a replica instance on port 8000
RUST_LOG=info cargo run -- --port 8000 --replicaof "127.0.0.1 6379"

# Run tests
RUST_LOG=info cargo test
```

## About

[![progress-banner](https://backend.codecrafters.io/progress/redis/46f107e9-b71f-4c03-bce1-dc82f11ba1d9)](https://app.codecrafters.io/users/codecrafters-bot?r=2qF)

This is my implementation of the Codecrafters ["Build Your Own Redis" Challenge](https://codecrafters.io/challenges/redis).

## Resources

- [tokio docs](https://docs.rs/tokio/latest/tokio/)
- [RESP spec](https://redis.io/docs/latest/develop/reference/protocol-spec/)
- [Redis commands reference](https://redis.io/docs/latest/commands/)
- [Redis explained article](https://architecturenotes.co/p/redis)
- [article about Redis being primarily single-threaded](https://builder.aws.com/content/2tfQijlXleV5iM6hDkSGkMJlpcd/the-engineering-wisdom-behind-rediss-single-threaded-design)
