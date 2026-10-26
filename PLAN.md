# Distributed Vector Search Service: 50 步规划

目标:Java 21 + Spring Boot + gRPC + Kafka + Redis,本地 docker-compose 跑通,最后上 AWS 压测。
简历里的数字(3,200 QPS / p99 92ms / 2M chunks / 断节点 0 失败)是**要实测达成的目标**,最终以实测为准。

每步完成后考 3 题:① 为什么这么做 ② 有哪些更好/更差的替代路线 ③ 基础知识扫描。

## Phase 0 地基(1-5)
1. 建 Gradle 多模块仓库:`proto` / `common` / `shard-node` / `coordinator` / `ingest` / `embed-worker`
2. docker-compose:Kafka(KRaft 模式,无 ZooKeeper)+ Redis + Prometheus + Grafana
3. 写 `.proto`:`Search`、`Upsert`、`Health` 三个 RPC,生成 Java stub
4. 选数据集(MS MARCO passages 或 Wikipedia),下载约 2M 段文本
5. 切 chunk:固定 token 窗口 + overlap,给每个 chunk 稳定 ID

## Phase 1 单机索引(6-12)
6. 暴力 kNN(余弦/点积)做 baseline,生成 ground truth 用来算 recall
7. 接入 HNSW(hnswlib-java),建索引、查 top-k
8. 调参 M / efConstruction / efSearch,画 recall@10 vs 延迟曲线
9. shard-node 起 gRPC server,单节点 Search/Upsert 跑通
10. 索引快照落盘 + 启动时恢复
11. Micrometer + Prometheus 指标:QPS、延迟直方图、索引大小
12. 单元测试 + 集成测试(Testcontainers)

## Phase 2 嵌入与写入管道(13-20)
13. Java 内跑 ONNX 嵌入模型(all-MiniLM-L6-v2,384 维,DJL / onnxruntime)
14. 批量嵌入 worker:攒批(按条数或时间窗)再推理
15. Kafka topic 设计:分区数、key 选择、副本因子
16. ingest 服务:REST/gRPC 收文档,写 `raw-chunks` topic
17. consumer group、offset 提交、at-least-once 语义
18. 幂等 upsert(按 chunk ID 覆盖),处理重复投递
19. 背压:批大小、poll 间隔、慢消费者
20. 死信队列(DLQ)+ 重试策略

## Phase 3 分片与副本(21-28)
21. 一致性哈希环 + 虚拟节点,手写并测分布均匀度
22. 分片放置:3 节点 × 2 副本(环上顺时针下一个不同物理节点)
23. 写路由:一条向量写到它的 2 个副本
24. 集群成员管理:Redis 存环元数据 + 心跳 TTL
25. Redis 查询缓存(query embedding → top-k),设 TTL 与失效策略
26. 副本一致性:写入确认策略(ONE / ALL),读取哪个副本
27. 加/减节点时的数据迁移(只搬受影响区间)
28. 分片层测试:哈希分布、迁移正确性

## Phase 4 查询路径(29-36)
29. coordinator 服务:接收查询、算 embedding
30. scatter:每个分片挑一个副本并发发请求
31. gather:多路 top-k 用堆合并成全局 top-k
32. gRPC deadline / 超时传递
33. 副本选择策略(轮询 vs power-of-two-choices)
34. hedged requests:超过 p95 还没回就向另一副本补发,取先到的
35. 故障检测 + 自动摘除/恢复节点
36. 部分结果策略:某分片全挂时返回什么

## Phase 5 压测与故障注入(37-44)
37. 灌入 2M chunks,测灌入吞吐
38. 装 k6 + xk6-grpc,写压测脚本(阶梯加压)
39. 基线测量:QPS、p50/p99、错误率
40. JFR 剖析 + 调优(GC、线程池、efSearch、对象分配)
41. hedging 开/关 A/B,对比 p99
42. 混沌:压测中 kill 一个节点
43. 验证 0 失败查询,分析原因链
44. 规模下的 recall@10 复核

## Phase 6 上云与收尾(45-50)
45. Jib 打容器镜像
46. AWS 部署(先算钱:EC2 + 自建 Kafka vs MSK,选够用最便宜)
47. AWS 上复跑压测,记录真实数字
48. Grafana 看板:延迟、QPS、节点健康
49. 英文 README + 架构图
50. 用实测数字核对简历 bullet + 面试追问演练
