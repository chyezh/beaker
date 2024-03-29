# beaker

[中文](./docs/CH_README.md)|[english](./README.md)

`beaker` is a distributed key-value database under development for learning `Rust`/`Database`/`Distributed System`.

## Usage

If you're a Rust programmer, build and it with `cargo`. 

    # run the database server at root path with log at info level
    RUST_LOG=info cargo run --release --bin server -- --root-path $path

    # run command line
    cargo run --release --bin cli

    # apply command use cli
    get key
    set key val
    del key
    ping msg

    # or using src/client in crate to apply command
    async {
        let client = Client::connect($addr).await?;
        client.ping(None).await?;
    }

If you want to install it, using `cargo install --path .`.

If you want in uninstall it, using `cargo uninstall beaker`.

## Repository content

Guide to modules:

- `src/engine`: A lsm-tree-based storage engine like `leveldb`.
- `src/server`: A implementation of database server.
- `src/cmd`: A redis-like command library.
- `src/cli`: A command line client tool.
- `src/client`: `Rust` Database Client.
- `src/resp`: A `RESP` protocol for exchanging data between client and server.
- `src/raft`: A `RAFT` protocol for exchanging data between one server node and another.

## Milestones

### Basic functional

- [x] Support `set`/`get`/`del` command.

### Standalone infrastructure

- [x] `lsm` tree based kv storage
  - [x] A `memtable+log` implementation
  - [x] A `sstable` implementation
  - [x] A `manifest` for managing sstables and logs
  - [x] Background task, `sstable compact`/`log dumping`/`expire file cleaning`...
  - [x] Database interface
- [x] `Resp` protocol
- [x] Database server
- [x] Rust client
- [x] command-line client tool
- [x] gentle shutdown

### Distribution infrastructure

- [ ] `Raft` protocol (brunch [beaker-v0.2.0](https://github.com/chyezh/beaker/tree/beaker-v0.2.0/src/raft))
  - [x] leader election
  - [ ] log replication
  - [ ] snapshot installation
  - [ ] configure modification

## References

- [levelDB](https://github.com/google/leveldb)
- [pingcap/talent-plan](https://github.com/pingcap/talent-plan)
- [tokio/mini-redis](https://github.com/tokio-rs/mini-redis)
- [Redis/Resp](https://redis.io/docs/reference/protocol-spec/)
- [Raft](https://raft.github.io/)