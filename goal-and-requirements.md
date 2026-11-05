---
id: goal-and-requirements
type: goal-and-requirements
status: active
title: Doris Lineage Plugins
---

# Doris Lineage Plugins

一个把 Doris 已分析好的血缘事件转成 OpenLineage 并投递出去的插件。

## 概述

面向运行 Apache Doris 的用户，实现 Doris 的 `LineagePlugin` SPI，并开源发布。Doris 已经从成功执行的 `INSERT INTO … SELECT`、`INSERT OVERWRITE TABLE … SELECT`、`CREATE TABLE AS SELECT` 的分析计划中抽出列级血缘，并把 `LineageInfo` 交给插件。本插件把该事件转成 OpenLineage，以 HTTP POST 发给配置的接收端，供目录展示这条边。它不自己解析 SQL，不连接 Doris FE，也不提供目录界面。

首版针对 Doris 4.0.6 构建并验证。4.0.6 是该框架的起始版本，不代表一个插件包兼容此后所有版本。其他版本按兼容矩阵发布。官方要求按所部署 FE 的准确版本或源码修订编译。

OpenLineage 插件在 `openlineage-plugin` 模块中开发，不占用仓库根目录作为唯一实现。每种插件形式各自一个模块。本文只规定 OpenLineage 这一模块；其他形式既未列入能力，也未被排除。

## 问题

Doris 产出血缘后就停了：不保留事件，不提供血缘查询 API，也不做可视化。用户若要把这些边放进 OpenLineage 目录，只能再解析一遍 SQL，或手工记下血缘。通用解析器可能存在方言覆盖和元数据解析差异，从而与 Doris 已完成的语义分析结果不一致；手工记录也会漏掉列级边。本插件的价值是直接复用 Doris 已经分析好的血缘，避免二次推导产生偏差。

Doris 的投递是异步且尽力而为：队列满时可能丢弃，不持久化、不重试，血缘失败也不回滚 DML。本插件不能承诺每一条成功写入都会进入目录。承诺从插件实际收到 `LineageInfo` 开始。

## 目标用户

- 跑受支持 Doris 写入的数据工程师，需要这些写入出现在已有的 OpenLineage 目录里。
- 运维该目录并安装 Doris 血缘插件的平台工程师。

## 要完成的事

当插件收到一条受支持写入的 `LineageInfo` 时，我要把它转成身份稳定、列级血缘完整的 OpenLineage 事件，并在有界的时间内 POST 到配置的接收端，以便在现有目录里看到这条边。

## 关键用户故事

数据工程师执行一条 Doris `INSERT INTO … SELECT` 或 `CTAS`。Doris 生成 `LineageInfo` 并调用本插件。插件发出 OpenLineage 事件，标明输入数据集、输出数据集、直接列依赖和间接列依赖。首版在 Marquez 上、不二次解析的情况下展示表级和列级边。

## 目标

- 对插件实际收到的三类写入事件，发出的 OpenLineage 事件通过固定版本 schema 校验，数据集标识稳定且不冲突，直接与间接列依赖符合映射契约。
- 在明确版本的 Doris 4.0.6 和一个 Marquez 版本上，安装插件并执行写入后，能查询并展示预期的表级与列级关系。
- 接收端超时或返回错误时，发送在规定的时间和资源限制内结束，失败可定位，重试使用同一个 Run ID。

## 非目标

- 不做 SQL 解析器，也不连接 FE。插件只消费 Doris SPI 传来的 `LineageInfo`。遍历 Doris 已提供的表达式对象以提取来源列，仍在此边界内。
- 不做目录界面。消费方是已有的 OpenLineage 后端。
- 不做跨引擎血缘。只实现 Doris 血缘 SPI。Spark、Flink、Hive 不在范围内。
- 不做通用血缘后端框架。日志、消息总线、Atlas、DataHub 直连发射器都不做。唯一投递方式是 OpenLineage HTTP。运行诊断日志不属于血缘后端。
- 不保证 Doris 未交给插件的写入进入目录。队列丢弃、不持久化、不重试是上游行为，不在本插件的承诺内。
- 不把 `INSERT … VALUES` 当作受支持写入。`VALUES` 等由 Doris 排除；插件不为此解析 SQL。

## 成功条件

三条各自可检查：

- **转换正确：** 对收到的三类写入事件，输出通过固定版本的 OpenLineage schema 校验。数据集标识、直接列依赖、数据集级与输出列级的间接依赖及类型符合映射契约。覆盖别名、目标列重排、多输入表达式、聚合、过滤、连接和窗口依赖。常量或无法解析的来源不凭空补边。
- **链路可用：** 在明确版本的 Doris 4.0.6 和一个 Marquez 版本上，安装、加载、配置插件并执行写入后，能查询并展示预期的表级与列级关系。
- **失败有界：** 接收端超时或返回错误时，发送在规定时间和资源限制内结束，失败可定位，重试身份保持一致。不无限阻塞，也不无限积压。

## 能力

- 作为 Doris `LineagePlugin` 加载，并在受支持的写入成功后接收 `LineageInfo`。否则 Doris 无法调用本插件。
- 把该 `LineageInfo` 转成 OpenLineage 事件，覆盖 `INSERT INTO … SELECT`、`INSERT OVERWRITE TABLE … SELECT` 和 `CTAS`。保留直接依赖，以及数据集级、输出列级的间接依赖和类型，映射到 OpenLineage `columnLineage` 的 `DIRECT`、`INDIRECT` 及相应子类型。
- 数据集、Job、Run 的身份满足：同一对象标识稳定，不同对象不冲突，列级引用与 inputs/outputs 的标识一致。同一张表不因请求落到不同 FE 而生成多个节点。拼接格式见 `architecture`。
- 每次成功回调补发一对 `START` 和 `COMPLETE`，不发 `FAIL`。顺序、时间和失败处理见 `architecture`。消费端兼容性由链路可用条件验证。
- 按 OpenLineage 规范，把事件以 HTTP POST 发到配置的接收端。插件不落库、不写消息队列。
- 接收端 URL、超时和鉴权从插件目录下的配置文件读取。插件用 `PluginContext.getProperties()` 中的 `plugin.path` 定位该文件。Doris 当前不透传这些参数：官方列出的属性只有 `plugin.name` 和 `plugin.path`。不把“由 Doris 透传 URL、超时、鉴权”当作现有能力。若以后改为 Doris 侧透传，须先改造 Doris，不能算作本插件已有的配置来源。
- HTTP 超时、拒绝和服务不可用时的行为有界：有限重试、明确的丢弃时机、可定位的失败记录。次数、超时和退避见 `architecture`。不无限阻塞，也不无限积压。发送失败不重新入队；重试耗尽后记录诊断信息并丢弃，继续处理后续事件。投递为尽力而为，不保证送达或恰好一次。
- 开源交付物包含安装、加载和配置说明，以及已验证的版本兼容矩阵。

