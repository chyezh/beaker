# beaker

[中文](./docs/CH_README.md)|[english](./README.md)

`Beaker`的开发目标是一个使用`Rust`开发的分布式`KV`数据库，用于学习`Rust`/数据库/分布式系统。

## 工程结构

相关模块导引如下：

- - `src/engine`: 基于`LSM-Tree`的存储引擎。
- `src/server`: 数据库服务器实现。
- `src/cmd`: 类似`Redis`的命令库。
- `src/cli`: 命令行客户端工具。
- `src/client`: `Rust`客户端。
- `src/resp`: `RESP`协议，用于客户端和服务端之间交换数据。
- `src/raft`: `RAFT`协议，用于数据库节点之间交换数据。

## 里程碑

### 基础功能

- [x] 支持基础的`get`/`set`/`del`等操作。

### 单机基础部分

- [x] 基于`LevelDB`设计思路的`KV`存储引擎
  - [x] `memtable+log`实现
  - [x] `manifest`实现
  - [x] `sstable`实现
  - [x] 包含`compact`/日志清理等一系列后台功能
  - [x] 功能整合，DB功能封装
- [x] 基于`Redis`的`Resp`的数据交换协议
- [x] 服务端入口
- [x] 客户端命令行工具
- [x] 优雅退出

### 分布式部分

- [ ] `Raft`协议（开发分支[beaker-v0.2.0](https://github.com/chyezh/beaker/tree/beaker-v0.2.0/src/raft))
  - [x] 主节点选举
  - [ ] 日志复制
  - [ ] 快照安装
  - [ ] 配置变更

## 参考与引用

- [levelDB](https://github.com/google/leveldb)
- [pingcap/talent-plan](https://github.com/pingcap/talent-plan)
- [tokio/mini-redis](https://github.com/tokio-rs/mini-redis)
- [Redis/Resp](https://redis.io/docs/reference/protocol-spec/)
- [Raft](https://raft.github.io/)