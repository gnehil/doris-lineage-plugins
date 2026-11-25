---
id: lineage-plugin-spec
type: module-design
status: active
title: Doris 血缘插件契约
parent: architecture
implements: [goal-and-requirements]
---

# Doris 血缘插件契约

两个 sink 共用 base 中的 Doris 适配器、事件编码、顺序与预算逻辑。模块和配置见 [README](README.md)。

## 责任

- 实现 `LineagePlugin`，并实现、注册 `LineagePluginFactory`，通过 ServiceLoader 加载。名称稳定，供 `activate_lineage_plugin` 引用。只实现 `LineagePlugin` 不会被发现。
- 安装用的是 shade 后的 jar。它的 `MANIFEST.MF` 主属性必须有 `Doris-Lineage-Plugin-Api-Version`，当前为 `1.0`，与当前 master 的 `lineage.plugin.api.version` 一致。目录加载器只比对 major；缺属性或 major 不同会在加载阶段拒绝，进不了 `initialize`。血缘这一族不把缺属性当成旧版放行。属性必须打在 shade 产物上，因为 shade 会重写 manifest。不读该属性的旧 FE 会忽略它。
- `initialize` 读取并校验 `{plugin.path}/<工厂名>.yml`，创建可复用的 HTTP 客户端或 Kafka producer。配置非法则插件加载失败。不读进程工作目录，也不把环境变量当作第二配置源。字段契约见「配置」。配置只在启动时读取，变更后须重启 FE。
- `eventFilter` 只读线程安全的本地状态。它在 DML 查询线程上调用，不读文件、不探测接收端、不等待锁。
- 把直接依赖映射为 `DIRECT`，间接依赖映射为 `INDIRECT`。只有 `LineageInfo` 已经给出子类型时才填写子类型。映射与降级见下节。事件由 `openlineage-java` 1.27.0 生成，其 `schemaURL` 固定为 OpenLineage `2-0-2`。升级客户端前，发出的事件仍必须使用该 schemaURL。
- OpenLineage 先发 `START`，接收端确认后再发 `COMPLETE`。Doris JSON 每次写入发送一条版本化记录。HTTP 客户端关闭自动重试和重定向，Kafka producer 使用 `acks=all`。发送次数、预算和丢弃只按 `architecture` 计数。事件模式见 `architecture`。

## 配置

两个配置文件均拒绝未知字段、重复键或第二份文档。

- `format` 可选，支持 `openlineage` 和 `doris-json`。HTTP 默认 `openlineage`，Kafka 默认 `doris-json`。
- HTTP `url`：完整的 http(s) POST 地址，例如 `http://marquez:5000/api/v1/lineage`。协议大小写不敏感。不再追加路径。端口可以不写；一旦写出，包括空端口，必须在 1 到 65535。
- `timeoutMs`：字段不出现时缺省 5000。出现时必须是正整数。`null`、`~`、空字符串、空白和小数都拒绝，不用缺省值掩盖。
- `cluster`：非空。数据集 namespace 为 `doris://{cluster}`。
- HTTP `auth.type` 只允许 `none` 或 `bearer`。`bearer` 的 `token` 必须是非空字符串，且 `Bearer ` 加 token 能放进 HTTP 头值：可见 ASCII、空格和 Latin-1。`none` 不能出现 `token` 字段，包括 `null` 和 `~`。

Kafka 的 `bootstrapServers`、`topic` 和 `producerProperties` 见 [README](README.md)。

## 映射与降级

常量没有来源列：不补边，不记转换失败。

`LineageInfo` 给出了依赖，但来源列无法解析：不凭空补边。记录转换诊断，至少包含 `query_id`、目标列和原因。这种输出可以发送已解析的边，但诊断必须标明转换不完整，不能与完整成功混在一起。

数据集级间接依赖也要带来源列。`columnLineage` 使用 facet `1-2-0`，其中每个 `InputField` 都必须有 `field`。发出该 facet 时 `fields` 必须存在，没有输出列边时为空对象。同一输出列上的多种依赖合并进同一个 `fields` 项，不互相覆盖。经视图读取时，列级引用用原始表和原始列，不用视图本身，以便和 `tableLineageSet` 里的输入表一致。输入和输出的 `schema.fields` 来自 Doris 基础表 schema（`getBaseSchema(false)`），不含隐藏列和同步物化视图的物理索引列。外部表不在转换时调用 `getBaseSchema`，只用事件里已经带有的列名和类型，避免缓存未命中时去远程拉 schema。读取失败或没有可用列时不发送空的 `schema.fields`，记 `schema unavailable`。类型用与列名同一身份的 `Column.getType().toSql()`。不用 Nereids `DataType.toString()`，所以外部表的 `STRUCT<id:INT>` 不会被写成调试字符串。没有这份 SQL 类型就省略，不编造。数据集级间接依赖只复制到本次 `targetColumns` 里的输出列。展开后的边数超过 10000 时不做逐列复制，保留紧凑的 `columnLineage.dataset`，并记诊断。Marquez 0.50.0 只读 `columnLineage.fields`，所以未超限时这些依赖同时出现在那些输出列的 `inputFields`。

数据集身份无法确定时不发送事件，并记录同一类诊断。

## 边界

FE 依赖使用标准 Maven `provided` 作用域，由部署的 FE 在运行时提供。base 编入两个 shaded JAR。HTTP 包包含 Apache HttpClient，Kafka 包包含 Kafka 客户端。

`eventFilter` 只检查初始化状态。`exec` 同步等待本批投递结果，不重新入队。血缘来源由 Doris 提供，插件不解析 SQL，也不连接 Doris 节点。
