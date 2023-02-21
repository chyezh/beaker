# beaker

`Beaker`的开发目标是一个使用`Rust`开发的分布式`KV`数据库。

## 里程碑

### 基础功能

- [x] 支持基础的`get`/`set`/`del`等操作。

### 基础部分

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

- [ ] `Raft`协议（进行中，当前已开发选举部分）

## 参考与引用

- [levelDB](https://github.com/google/leveldb)
- [pingcap/talent-plan](https://github.com/pingcap/talent-plan)
- [tokio/mini-redis](https://github.com/tokio-rs/mini-redis)
- [Redis/Resp](https://redis.io/docs/reference/protocol-spec/)
- [Raft](https://raft.github.io/)