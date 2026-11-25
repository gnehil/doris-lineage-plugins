# openlineage-plugin

把 Doris 4.0.6 的 `LineageInfo` 转成 OpenLineage，并 POST 到配置的接收端。

## 编译

契约测试不需要 FE：

```bash
mvn test
```

可安装的插件包必须对着所部署 FE 的 lib 编译：

```bash
mvn -Ddoris.fe.lib=/path/to/fe/lib clean verify
```

`verify` 会检查插件 JAR 中的工厂类和 SPI 注册文件。`fe-core` 和 `fe-extension-spi` 只在编译期使用，不会打进插件包。

## 安装

Doris 只扫描 `${plugin_dir}/lineage` 的直接子目录。把 jar 和 `openlineage.yml` 都放在 `${plugin_dir}/lineage/openlineage/`。jar 的 `MANIFEST.MF` 带 `Doris-Lineage-Plugin-Api-Version: 1.0`；不要换成去掉这段声明的包，否则当前 FE 会在加载时跳过插件。

```yaml
url: http://marquez:5000/api/v1/lineage
timeoutMs: 5000
cluster: warehouse-a
auth:
  type: bearer
  token: secret
```

`url` 是完整 POST 地址，不再追加路径。`auth.type` 只能是 `none` 或 `bearer`。

在 `fe.conf` 里启用：

```text
activate_lineage_plugin=openlineage
```

改配置后重启 FE。
