---
id: architecture
type: architecture-design
status: active
title: Doris Lineage Plugins 架构
parent: goal-and-requirements
---

# 架构

仓库是一组彼此独立的 Doris 血缘插件。当前只有 `openlineage-plugin`。它实现 Doris `LineagePlugin`，把收到的 `LineageInfo` 转成一对 OpenLineage 事件，再 HTTP POST 到配置的接收端。产品目标见 `goal-and-requirements`。

## 拓扑

```
Doris FE
  LineageEventProcessor（异步、可丢弃）
    → openlineage-plugin.exec(LineageInfo)
        → 转换
        → OpenLineage HTTP
            → 接收端
```

FE 负责语义分析和入队。插件不解析 SQL，不连接 FE，不持有自己的持久队列。`exec` 返回前必须结束发送；血缘失败不回滚 DML，这是上游行为。

其他插件形式若出现，是同级模块，不依赖 `openlineage-plugin`，也不被它依赖。在第二个模块出现之前，不设共享库。

## 模块

`openlineage-plugin` 是唯一在本文范围内的模块。责任与边界见 `openlineage-plugin`。

## 不变量

- 承诺从 `exec` 收到 `LineageInfo` 开始。Doris 队列丢弃的事件不在承诺内。
- 只消费 Doris 为受支持写入生成的事件。`VALUES` 等由 Doris 排除；插件不为此解析 SQL。
- 每次成功回调补发一对 `START` 和 `COMPLETE`，不发 `FAIL`。插件看不到失败的 DML。顺序、时间和失败处理见「事件模式」。
- 列级边只来自 `LineageInfo` 已给出的直接依赖和间接依赖。常量没有来源是正常情况，不补边，也不记为失败。已有依赖无法解析时不补边，并记录转换诊断，不得记成完整转换成功。
- 同一逻辑表在所有 FE 上使用同一个数据集身份。身份不包含 FE 主机名。
- 同一次 Query ID 的重试复用同一个 Run ID。
- 发送在单事件总处理时限内结束。不无限阻塞，也不在插件内再入队。
- 配置只来自 `{plugin.path}` 下的文件。`PluginContext` 不透传接收端 URL、超时和鉴权。
- 投递为尽力而为。不保证送达，也不保证恰好一次。

## 身份

数据集 namespace 是配置项 `cluster`，形如 `doris://{cluster}`。`cluster` 由运维指定一个稳定集群名，不从 FE 地址推导。数据集 name 是 `{catalog}.{database}.{table}`，catalog 始终保留，避免 internal 与外部 catalog 撞名。

Job namespace 与数据集 namespace 相同。Job name 等于输出数据集的 name。对同一张输出表的多次写入是同一个 Job 的多次 Run，而不是每个 SQL 一个 Job。

Run ID：Query ID 已是 UUID 时直接使用。否则用 UUIDv5，输入是无歧义编码的 `(cluster namespace, job name, query ID)`。没有 Query ID 时，为本次 `exec` 生成随机 UUID，同一次发送的重试复用它；再次收到同一逻辑事件时无法据此去重。重试始终复用同一份事件和同一个 Run ID。

列级 facet 里的数据集引用必须使用上面的 namespace 和 name。

## 失败

`exec` 在 Doris 血缘分发线程上同步发送。该线程是单 worker：慢请求会拖住后续事件，也会拖住同一分发器上的其他血缘插件。插件不另建队列。吞吐取决于接收端响应速度；Doris 自己的队列满了仍可能丢事件，那不是本插件的队列。

单事件发送时限从第一次发起 HTTP 之前的单调时钟起算，缺省 15 秒。转换和序列化不计入这 15 秒，但它们同样占着分发线程。每次请求的超时是总超时，覆盖连接和等待响应，不只是读超时；实际总超时为 `min(配置超时, 剩余发送预算)`，配置超时缺省 5 秒。最多重试 2 次，不保证用满。等待会耗尽剩余预算则结束，不再发送。OpenLineage HTTP 客户端的自动重试必须关闭，发送次数只由这里计数。

成功以最终 HTTP 2xx 为准。仅超时、429 和 5xx 重试。其他 4xx 立即丢弃。连接拒绝、DNS 和 TLS 错误不算超时，立即丢弃并记录原因。禁止自动跟随 3xx，把 3xx 记为失败且不重试。超时重试可能重复投递：接收端可能已接收，只是响应没回来。

有效 `Retry-After` 超过剩余发送预算时，放弃重试，不截断后提前重试。缺失或无效时，第一次重试前等待 200ms，第二次前等待 500ms；该等待也受剩余预算约束。

发送失败不重新入队。重试耗尽或触到上述立即丢弃条件后，记录诊断信息并丢弃，然后继续处理后续事件。诊断字段为 `query_id`、`target_table`、`attempts`、`http_status`、`error_type`。没有 HTTP 响应时 `http_status` 为空，不编造状态码。不记录鉴权头。

## 验收消费端

首版只对一个能展示列级血缘的 Marquez 发行版做端到端验收，版本在验收时锁定，不在本文猜测版本号。验收要看到这对 `START` / `COMPLETE` 被接收、Run 状态可读、列级边能展示。DataHub 不因此增加发射器，也不写入首版承诺。

## 事件模式

事件按 OpenLineage schema `2-0-2` 校验，`columnLineage` 使用该 schema 中的定义。schema 要求每个 Run 有一个 `START` 和一个终态事件。单条 `COMPLETE` 通过 JSON 校验，不等于满足生命周期。

收到成功回调后，先发 `START`，拿到 HTTP 2xx 后再发 `COMPLETE`。不发 `FAIL`。两条事件共用同一个 Run ID 和同一份数据集与列级血缘，并共用 15 秒发送预算。每个事件最多重试 2 次，预算耗尽则提前结束。

`START` 在预算内仍未拿到 2xx：丢弃，不发 `COMPLETE`。`START` 已成功而 `COMPLETE` 失败：只重试 `COMPLETE`，不重发 `START`。

`eventTime`：4.0.6 的 `LineageContext` 若同时给出查询开始时间和耗时，`START` 用开始时间，`COMPLETE` 用开始时间加耗时。耗时为零可以。缺任一字段，或耗时为负，则两条都用插件收到 `LineageInfo` 的时刻，并记录诊断 `start_time_source=receive_time`，标明 `START` 不是真实开始。两条事件不混用两套时间。公开文档能读到的 getter 表在截断前没有列出这两个字段，因此“有则用”取决于 4.0.6 API，不把字段名写成已核对。

## 构建

针对 Doris 4.0.6 构建。Doris API 依赖包括 `fe-core` 和 `fe-extension-spi`：`LineagePlugin`、`LineagePluginFactory` 和 `LineageInfo` 在 `fe-core`，通用插件接口在 `fe-extension-spi`。二者都由 FE 在运行时提供，以 `provided` 依赖编译，不打入插件包。插件自带 OpenLineage、HTTP 等运行依赖，并验证依赖隔离和实际加载。其他 Doris 版本不默认为兼容。

必须实现并注册 `LineagePluginFactory`，通过 ServiceLoader 加载。只实现 `LineagePlugin` 不会被发现。
