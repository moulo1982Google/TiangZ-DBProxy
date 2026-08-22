# DBProxy可观测性

DBProxy使用独立HTTP监听暴露Prometheus指标，Prometheus负责抓取和告警，Grafana只负责查询与展示：

```text
TiangZ Process -> DBProxy TCP 7800/7801
                       |
                       +-> /live /ready /metrics 9090/9091
                                      |
                                  Prometheus
                                      |
                                    Grafana
```

观测HTTP端口不接受DBProxy业务令牌，也不会输出数据库连接串、认证令牌、RecordKey、玩家ID、requestId或operationId。它必须只绑定本机或运维内网，禁止通过公网Nginx转发。

## 本地启动

从`deploy/local/.env.example`复制开发环境变量后执行：

```powershell
docker compose --env-file deploy/local/.env -f deploy/local/docker-compose.yml up -d
$env:DBPROXY_AUTH_TOKEN = "tiangz-dbproxy-local-token-2026"
Start-Process powershell -ArgumentList "-NoProfile -ExecutionPolicy Bypass -File tools/run_local.ps1 -ConfigFile configs/local-1.json" -WindowStyle Hidden
Start-Process powershell -ArgumentList "-NoProfile -ExecutionPolicy Bypass -File tools/run_local.ps1 -ConfigFile configs/local-2.json" -WindowStyle Hidden
```

访问入口：

- Grafana：`http://127.0.0.1:3000`
- Prometheus：`http://127.0.0.1:9095`
- DBProxy 1：`http://127.0.0.1:9090/metrics`
- DBProxy 2：`http://127.0.0.1:9091/metrics`

Grafana会自动配置Prometheus数据源并加载`TiangZ DBProxy Overview`，不需要手工导入JSON。

## 指标边界

| 指标 | 含义 |
| --- | --- |
| `dbproxy_live` / `dbproxy_ready` | 实例存活与接流量状态 |
| `dbproxy_connections_total` / `dbproxy_connections_active` | TCP连接累计值与当前值 |
| `dbproxy_handshake_rejections_total` | 按协议、令牌或客户端名称分类的握手拒绝 |
| `dbproxy_requests_in_flight` | 当前执行中的RPC数量 |
| `dbproxy_rpc_requests_total` | 按固定操作名统计的RPC请求 |
| `dbproxy_rpc_failures_total` | 按固定操作名统计的失败 |
| `dbproxy_rpc_errors_total` | 按固定错误码分类的失败原因 |
| `dbproxy_rpc_records_total` | 批量RPC处理的逻辑记录数 |
| `dbproxy_rpc_duration_seconds` | 可计算P50/P95/P99的Histogram |
| `dbproxy_backlog_polls_total` | Backlog提交、空轮询与失败次数 |
| `dbproxy_backlog_processing_seconds_total` | Backlog处理累计时间 |
| `tiangz_dbproxy_endpoint_*` | TiangZ侧连接尝试、请求失败与Endpoint切换 |

`operation`、`code`和Endpoint序号都是固定低基数标签。禁止为了排查单个玩家而增加`playerId`、`namespace`、`recordKey`或`operationId`标签；单次请求关联只进入Debug结构化日志。

## 默认告警

本地规则文件`deploy/local/observability/alert-rules.yml`包含：

- 实例30秒无法抓取；
- 实例持续30秒未Ready；
- 存储错误持续出现；
- P99持续5分钟超过100ms；
- Redis普通快照Backlog持续处理失败。

本地Prometheus只计算告警，不配置通知渠道。生产环境由运维侧Alertmanager或云监控接收这些规则。

## 生产部署

两个DBProxy实例使用不同观测端口，例如`127.0.0.1:9090`和`127.0.0.1:9091`。Prometheus可以部署在同机，也可以通过防火墙允许专用监控网段访问。业务TCP、观测HTTP和PostgreSQL/Redis端口必须分别管理，不能因为Grafana需要指标就把任一端口暴露公网。

Dashboard展示的是DBProxy服务和TiangZ客户端行为。PostgreSQL与Redis自身的连接池、慢查询、Buffer和实例资源仍应使用云厂商监控或官方Exporter；DBProxy不会冒充数据库内部指标的权威来源。

本地Prometheus还会抓取TiangZ all-in-one的`7600`以及`cluster-dbproxy`中启用持久化的Process健康端口。未启动的开发拓扑会显示为Down，但不会触发`tiangz-dbproxy`实例告警；正式部署应通过服务发现或独立静态目标清单替换这些本机示例端口。
