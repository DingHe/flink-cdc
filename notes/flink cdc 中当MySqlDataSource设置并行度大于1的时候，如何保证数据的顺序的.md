# 1、概述

在 Flink CDC 中，当 MySqlDataSource 的并行度大于 1 时，保证数据顺序性主要依赖于 两个核心阶段 的设计：快照阶段（Snapshot Phase） 的切片顺序保证和 增量阶段（Binlog Phase） 的数据分发机制。

以下是详细的技术实现原理：

源码逻辑在： MySqlHybridSplitAssigner

## 1.1、快照阶段：增量快照读取算法 (Incremental Snapshot)

Flink CDC 2.0 引入了增量快照算法，这是支持高并行度且保证顺序的基础。

数据切片（Chunking）： 数据表会被根据主键（Primary Key）切分成多个 Snapshot Chunks。这些 Chunk 可以并行分发给不同的 Subtask 读取。

低水位线与高水位线 (Watermark Mechanism)： 为了保证在读取 Chunk 存量数据期间发生的增量变更不丢失且顺序正确，每个 Chunk 在读取前后会记录 Binlog 的位置：

记录 low watermark（当前 Binlog 位置）。

读取 Chunk 内的所有行数据。

记录 high watermark。

修正（Reconciliation）：如果在读取过程中该 Chunk 范围内的数据发生了变化，Flink 会根据这段时间产生的 Binlog 对 Chunk 结果进行修正，确保交接给下游的数据是最终一致且有序的。


## 1.2、增量阶段：Binlog 的单线程读取与分发

这是保证“顺序”最关键的一点。尽管 Source 的并行度可以设为大于 1，但在读取 Binlog 阶段，其内部逻辑会有所不同：单节点读取：MySQL 的 Binlog 本质上是一个单线程的顺序日志。在 Flink CDC 中，即使 Source 并行度为 $N$，通常也只有 一个特定的 Enumerator (协调器) 负责拉取 Binlog，或者由一个指定的 Task 负责 Binlog 阶段。顺序广播/分发：一旦 Binlog 被读取，Flink 必须确保同一条记录（具有相同主键）的变更按顺序发送到下游。

## 1.3、下游算子的 KeyBy 分区 (核心保证)

Source 读完数据后，如何分发给并行的下游（如 Sink）而不乱序？

按主键 Hash (Shuffling)： Flink CDC 内部（或在 DataSinkTranslator 转换时）会自动或建议按照 主键 (Primary Key) 进行 keyBy。

原理： 由于 Hash(PK) % Parallelism 的结果是固定的，同一主键的所有变更（INSERT/UPDATE/DELETE）必然会发送到同一个下游 Subtask 中。

FIFO 队列： 在 Flink 的网络传输层中，同一个通道（Channel）的数据遵循先进先出（FIFO）原则。因此，只要同一行数据进入了同一个算子实例，它们的处理顺序就与 Binlog 中的产生顺序完全一致。

# 2、“增量快照算法”（Incremental Snapshot Algorithm）

“增量快照算法”（Incremental Snapshot Algorithm）是 Flink CDC 核心的黑科技，它解决了传统 CDC 锁表（Locking）和无法断点续传的问题。其核心逻辑在于**“如何让存量数据（历史快照）与增量数据（Binlog）在时间线上无缝重合且互不冲突”**。

我们可以通过以下几个核心步骤来拆解这个过程：

1. 核心概念：什么是水位线（Watermark）？
   这里的“水位线”并非 Flink Window 中的时间水位线，而是 Binlog 的偏移量（Offset）。

低水位线（Low Watermark, LW）：开始读取某一段存量数据（Chunk）之前，记录下当前的 Binlog 位置。

高水位线（High Watermark, HW）：读取完这段存量数据后，再次记录当前的 Binlog 位置。

在这两个水位线之间产生的 Binlog，就是在读取存量数据的过程中，数据库里发生的实时变更。

算法三部曲：读取、修正、对齐
为了保证数据一致性，算法会对每个数据切片（Chunk）执行以下流程：

第一步：打点与扫描 (The Chunk Scan)
记录 LW：获取当前 Binlog 的位置。

执行 Select：执行类似 SELECT * FROM table WHERE id >= 1 AND id < 100。

记录 HW：读取完毕后，再次获取 Binlog 的位置。 此时，拿到的 Chunk 数据是不准确的，因为在执行 SELECT 期间，数据可能已经被修改了。

第二步：Binlog 记录提取
如果在 LW 到 HW 之间，Binlog 记录了关于 id 在 1 到 100 之间的变更事件，这些事件会被收集起来。

第三步：数据修正（Reconciliation）
这是最关键的一步。Flink 会将“静态的快照”与“动态的变更”进行对比：

如果快照里的数据在 Binlog 里也有：以 Binlog 为准，覆盖快照数据。

如果快照里没有，但 Binlog 里新增了：将其作为增量数据合并进来。

如果快照里有，但 Binlog 里删除了：从快照结果中剔除。

为什么这个算法能保证顺序且不乱序？
传统的做法（如 Debezium）通常是：先读完整个表的快照，再开始读 Binlog。这导致了两个问题：

快照阶段无法并行：只能单线程读，否则无法确定 Binlog 衔接点。

阻塞：必须锁表来保证快照一致性。

增量快照算法的优势在于：

局部一致性替代全局一致性：它不要求整个表在某一刻一致，只要求每个 Chunk（切片）在自己的 LW 和 HW 之间达成一致。

无锁并行：不同的并行 Subtask 可以各司其职，处理不同的 Chunk。处理完 Chunk 后，只需将对应的 HW 汇报给协调器（Coordinator）。

最终对齐：当所有的 Chunk（存量阶段）都读取并修正完毕后，系统会记录下最后一个 Chunk 的最高 HW。从这个点开始，系统切换到 纯增量读取模式，不再需要读取快照。