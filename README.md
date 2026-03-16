![bitalos](./docs/bitalos.png)

### Bitalostored is a high-performance distributed storage system, compatible with Redis protocol. [中文版](./README_CN.md)

## Introduction

- Bitalostored is a high-performance distributed storage system, core engine based on [bitalosdb](https://github.com/zuoyebang/bitalosdb/blob/main/README.md), compatible with Redis protocol. As an alternative to Redis, it stores data with low-cost hard disk instead of expensive memory, takes full advantage of multi-core and provides excellent single-core performance, which can significantly reduce service costs.

- Bitalostored contains three main projects: dashboard (visual management platform), stored (storage service), and proxy (proxy service). Current open-source version is stable, and provides a complete industrial grade solution.

## Main Creator

- Author: Xu Ruibo(hustxurb@163.com), joined Zuoyebang in December 2018 (working till now), is responsible for live class middle platform and Zuoyebang platform, and leads the storage technology team to develop Bitalos from 0 to 1.

- Contributors: Xing Fu(wzxingfu@gmail.com), Lu Wenwei(422213023@qq.com), Liu Fang(killcode13@sina.com), Li Jingchen(cokin.lee@outlook.com)

# Practice (Zuoyebang)

- Application: supports Zuoyebang's online & offline KV storage

- Peak traffic: QPS(30 Million), Network(8000Gbps)

- Storage data: 1PB

- Average reading time: 100μs, average writing time: 150μs

- Availability: 99.999%

- Stability: Since V1.0 was released in 2019, there have been zero online incidents

## Key Technology

- Compatible with Redis protocol, low integration cost. Supports most commands, including LUA, distributed transactions.

- Efficient horizontal scaling, supporting up to 1024 shards and enabling shard expansion in seconds.

- Multi-cloud disaster recovery, supporting multi-data room or multi-cloud deployment & management, with a complete downgrade and disaster recovery solution.

- Multi-master architecture (supported in Enterprise Edition), based on CRDT, optimized data synchronization and consistency strategies, ensuring adaptive conflict resolution during multi-master writes and achieving eventual consistency.

- High-performance data consistency architecture, based on Bitalos-Raft, with deeply optimized Raft protocol, which greatly improves write performance, and features more stable election strategy and data synchronization process.
    - High-performance log engine: bitaloslog, featuring log indexing based on bitalostree, supporting high-throughput write operations.

- High-performance kernel equipped with self-developed KV engine: bitalosdb.
    - High-performance compressed indexing technology: bitalostree, a B+ tree based on ultra-large pages, featuring innovative index compression that eliminates write amplification in B+ trees and maximizes read performance.

    - High-performance KV index, vector computation implemented based on ASM assembly, with significantly improved performance.

    - High-performance K-KV index, multi-level vector index based on bitalostree, balancing index compression ratio and retrieval performance.

    - High-performance KV separation technology: bithash, based on a compact index structure, with O(1) retrieval efficiency and capable of independent GC.

    - High-performance storage structure that compresses Redis composite data types, significantly reducing I/O costs and improving system throughput.

## Quick deployment

- Applicable scenarios: Deploy a test cluster on a single machine(machine needs to be connected to the Internet), experience the functions of all components(dashboard, proxy, and stored), and cluster operation and maintenance

- Deployment script: install.sh, follow the prompts to enter the number of shards (group), the number of slave nodes (slave), and the number of witness nodes (witness); the default number: proxy * 1, group * 2 (master * 2, slave * 2 , witness * 2)

- Admin web: 127.0.0.1:8080, both of default user&password are demo

- Service address: 127.0.0.1:8790, use command: redis-cli -h 127.0.0.1 -p 8790

- Uninstall script: uninstall.sh

## Performance

- Currently, there are several well-known open-source storage systems (Redis-compatible) that serve as alternatives to Redis. We have selected two high-performance products (t\*d\* and \*i\*a) for performance comparison.

### Hardware

```
CPU:    Intel(R) Xeon(R) Platinum 8255C CPU @ 2.50GHz
Memory: 384GB
Disk:   2*3.5TB NVMe SSD
```

### Program

- Benchmark: memtier_benchmark (redis official tool)

- NoSQL Program: thread number(8), cgroup cpu(8 core)

- Command args: 2 data spec

```
--data-size=1024 --key-maximum=40672038 -t 16 -c 32 -n 81920
--data-size=128 --key-maximum=335544320 -t 16 -c 32 -n 655360
```

- Command (e.g., --data-size=1024)

```
./memtier_benchmark -t 16 -c 32 -s 127.0.0.1 -p xxxx --distinct-client-seed --command="set __key__ __data__" --key-prefix="performance_test_key_prefix_" --key-minimum=1 --key-maximum=40672038 --random-data --data-size=1024 -n 81920
./memtier_benchmark -t 16 -c 32 -s 127.0.0.1 -p xxxx --distinct-client-seed --command="get __key__" --key-prefix="performance_test_key_prefix_" --key-minimum=1 --key-maximum=40672038 --test-time=300
./memtier_benchmark -t 16 -c 32 -s 127.0.0.1 -p xxxx --distinct-client-seed --command="incr __key__" --key-prefix="int_" --key-minimum=1 --key-maximum=40672038 --random-data --data-size=1024 -n 81920
./memtier_benchmark -t 16 -c 32 -s 127.0.0.1 -p xxxx --distinct-client-seed --command="lpush __key__ __data__" --key-prefix="list_" --key-minimum=1 --key-maximum=40672038 --random-data --data-size=1024 -n 81920
./memtier_benchmark -t 16 -c 32 -s 127.0.0.1 -p xxxx --distinct-client-seed --command="sadd __key__ __data__" --key-prefix="set_" --key-minimum=1 --key-maximum=40672038 --random-data --data-size=1024 -n 81920
./memtier_benchmark -t 16 -c 32 -s 127.0.0.1 -p xxxx --distinct-client-seed --command="zadd __key__ __key__ __data__" --key-prefix="" --key-minimum=1 --key-maximum=40672038 --random-data --data-size=1024 -n 81920
./memtier_benchmark -t 16 -c 32 -s 127.0.0.1 -p xxxx --distinct-client-seed --command="hset __key__ __data__ __key__" --key-prefix="hash_" --key-minimum=1 --key-maximum=40672038 --random-data --data-size=1024 -n 81920
```

incr is irrelevant to data size, only needs to be tested once.


### Data

- Total data size：40GB

- Comparison dimensions： comand（SET、GET、LPUSH、SADD、ZADD、HSET） x value-size&count（1KB & 40,672,000、128B & 335,544,320）, INCR

### Config

- t\*d\* & \*i\*a

```
Threads:8
Memtable：1GB
WAL：disable
Binlog：disable
Cache：40GB

Other parameters are set as same as the official recommended benchmark configuration
```

- bitalostored

```
Threads:8
Memtable：1GB
WAL：disable
Raftlog：disable
```

### Result

- QPS

![benchmark](./docs/benchmark-bitalostored-qps.png)

## Document

Technical architecture and documentation, refer to the official website: bitalos.zuoyebang.com

## Technology accumulation(bitalosearch)

- High performance distributed search & analysis engine, SQL protocol, focusing on AP scenarios, and has certain TP capabilities. It is being practiced internally, and the open source plan is to be determined

- Compared to elasticsearch, bitalosearch has significant cost advantages. Hard disk consumption is saved 30%; data writing performance is improved by 25%; for complex analysis logic, query performance is improved by 20% to 500%