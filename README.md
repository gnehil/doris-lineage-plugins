# Doris 血缘插件

两种可独立安装的 sink 接收 Apache Doris 4.0.6 的 `LineageInfo`。HTTP 和 Kafka 均可输出 `openlineage` 或 `doris-json`。

## 准备开发环境

使用 JDK 17 和 Maven。在内部 Maven 仓库已有匹配的 `org.apache.doris:fe-core`、`fe-common` 和 `fe-extension-spi` 4.0.6 构件时，可以直接导入根 `pom.xml` 或运行 Maven。否则，从 [Doris 4.0.6 发行包](https://github.com/apache/doris/releases/tag/4.0.6)准备 FE 库：

```bash
cp .env.example .env
# 编辑 .env，将 DORIS_FE_LIB 指向发行包的绝对路径 fe/lib
./scripts/bootstrap-fe-deps.sh
```

环境变量 `DORIS_FE_LIB` 优先于 `.env`。脚本检查 FE 的版本和血缘 API 类，再按 POM 中的坐标安装 JAR。Guava 从常规 Maven 仓库解析。可向脚本传递 `-s` 或 `-Dmaven.repo.local=...`，IDE 和命令行应使用同一套 Maven 设置。初始化完成后即可移开 `.env`。

```bash
mvn clean test
mvn -pl doris-lineage-plugin-http -am clean verify
mvn -pl doris-lineage-plugin-kafka -am clean verify
mvn clean verify
```

可部署文件是对应 sink 的 `target/doris-lineage-plugin-<sink>-<version>.jar` 和同目录的 `*.yml.example`。发布时只取这个 shaded JAR。构建和打包验证覆盖工厂类、SPI、manifest 及运行时依赖边界。

## 安装与配置

| 项目 | HTTP | Kafka |
|---|---|---|
| 安装目录 | `${plugin_dir}/lineage/openlineage/` | `${plugin_dir}/lineage/kafka/` |
| `activate_lineage_plugin` 的值 | `openlineage` | `kafka` |
| 运行时配置 | [openlineage.yml.example](doris-lineage-plugin-http/openlineage.yml.example)，安装时命名为 `openlineage.yml` | [kafka.yml.example](doris-lineage-plugin-kafka/kafka.yml.example)，安装时命名为 `kafka.yml` |
| 默认格式 | `openlineage` | `doris-json` |
| sink 专用字段 | 完整 POST `url`，以及 `auth.type` 和可选的 bearer `auth.token` | `bootstrapServers`、`topic`，以及可选的 TLS/SASL `producerProperties` |

将 shaded JAR 和配置放在同一插件目录中，在 `fe.conf` 选定工厂名，然后重启 FE。已部署 HTTP 插件可直接替换原 JAR，继续使用原来的目录、激活值和 YAML。部署预构建 JAR 时只需要 FE、JAR 和 YAML。

`cluster` 为稳定集群名，OpenLineage namespace 形如 `doris://warehouse-a`。显式设置 `format: openlineage` 或 `format: doris-json` 可切换编码。两个 sink 的 `timeoutMs` 默认 5000 毫秒，每次请求或 Kafka 投递都受同批事件的 15 秒剩余预算约束。Kafka 的 `bootstrapServers` 接受逗号分隔字符串和 YAML 列表。 `producerProperties` 接受 Kafka 客户端的 TLS/SASL 配置，确认策略和发送超时由插件管理。

OpenLineage 在 `START` 确认后才发送 `COMPLETE`。Doris JSON 每次写入发一条 `schemaVersion: 1` 记录，包含 `queryId`、`runId`、`cluster`、`inputs`、`output`、`targetColumns`、`directDependencies`、`columnIndirectDependencies` 和 `datasetIndirectDependencies`。Kafka 总是以 Run ID 为 key，要求 `acks=all`。确认超时后停止本批后续发送，先前提交给 broker 的消息仍可能到达。

更多映射细节见 [插件契约](SPEC.md)。
