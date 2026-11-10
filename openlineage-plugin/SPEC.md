---
id: openlineage-plugin
type: module-design
status: active
title: OpenLineage 插件
parent: architecture
implements: [goal-and-requirements]
---

# OpenLineage 插件

把 Doris 交给本插件的 `LineageInfo` 转成 OpenLineage 事件并 POST 出去。跨模块不变量见 `architecture`。

## 责任

- 实现 `LineagePlugin`，并实现、注册 `LineagePluginFactory`，通过 ServiceLoader 加载。名称稳定，供 `activate_lineage_plugin` 引用。只实现 `LineagePlugin` 不会被发现。
- `initialize` 读取并校验 `{plugin.path}/openlineage.yml`，创建可复用的 HTTP 客户端。配置非法则插件加载失败。不读进程工作目录，也不把环境变量当作第二配置源。字段契约见「配置」。配置只在启动时读取，变更后须重启 FE。
- `eventFilter` 只读线程安全的本地状态。它在 DML 查询线程上调用，不读文件、不探测接收端、不等待锁。
- 把直接依赖映射为 `DIRECT`，间接依赖映射为 `INDIRECT`。只有 `LineageInfo` 已经给出子类型时才填写子类型。映射与降级见下节。事件由 `openlineage-java` 1.27.0 生成，其 `schemaURL` 固定为 OpenLineage `2-0-2`。升级客户端前，发出的事件仍必须使用该 schemaURL。
- 先发 `START`，成功后再发 `COMPLETE`。每个插件实例有自己的 HTTP 客户端，在该实例上关闭自动重试和重定向，不靠 JVM 全局属性。发送次数、预算和丢弃只按 `architecture` 计数。事件模式见 `architecture`。

## 配置

`openlineage.yml` 只允许四个字段，多一个字段、重复键或第二份文档都使加载失败。

- `url`：完整的 http(s) POST 地址，例如 `http://marquez:5000/api/v1/lineage`。协议大小写不敏感。不再追加路径。端口可以不写；一旦写出，包括空端口，必须在 1 到 65535。
- `timeoutMs`：字段不出现时缺省 5000。出现时必须是正整数。`null`、`~`、空字符串、空白和小数都拒绝，不用缺省值掩盖。
- `cluster`：非空。数据集 namespace 为 `doris://{cluster}`。
- `auth.type` 只允许 `none` 或 `bearer`。`bearer` 的 `token` 必须是非空字符串，且 `Bearer ` 加 token 能放进 HTTP 头值：可见 ASCII、空格和 Latin-1。`none` 不能出现 `token` 字段，包括 `null` 和 `~`。

## 映射与降级

常量没有来源列：不补边，不记转换失败。

`LineageInfo` 给出了依赖，但来源列无法解析：不凭空补边。记录转换诊断，至少包含 `query_id`、目标列和原因。这种输出可以发送已解析的边，但诊断必须标明转换不完整，不能与完整成功混在一起。

数据集级间接依赖也要带来源列。`columnLineage` 使用 facet `1-2-0`，其中每个 `InputField` 都必须有 `field`。发出该 facet 时 `fields` 必须存在，没有输出列边时为空对象。同一输出列上的多种依赖合并进同一个 `fields` 项，不互相覆盖。经视图读取时，列级引用用原始表和原始列，不用视图本身，以便和 `tableLineageSet` 里的输入表一致。输入和输出都带 `schema.fields`，名字覆盖列级边用到的列。没有列类型时类型记为 `STRING`，只为了让 Marquez 0.50.0 能建字段记录。

数据集身份无法确定时不发送事件，并记录同一类诊断。

## 边界

允许依赖：Doris `fe-core` 与 `fe-extension-spi`（`provided`，不打入插件包）、插件自带的 OpenLineage Java 客户端及其 HTTP 运行库。只有 `src/doris` 里的适配器可以 import Doris 类型，并且只在 `-Ddoris.fe.lib` 指向 FE lib 时编译。默认契约测试不依赖 FE。

禁止：解析 SQL；连接 FE 或任何 Doris 节点；写目录、消息队列、Atlas 或 DataHub 专用 API；依赖本仓库未来的同级模块；在 `exec` 之外保留待发送事件；在 `eventFilter` 中做 I/O。
