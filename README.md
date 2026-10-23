# TVBox 播放源自动维护

每天自动收集多个 TVBox 上游配置，合并去重、逐站验活、剔除失效站点，生成一份新的 `tvbox.json` 供电视盒子订阅。盒子只需把配置地址指向本服务，从此不用再手动换源。

## 工作流程

1. **收集**：按 `sources.txt` 清单抓取各上游配置（容错解析：原始 JSON → 清理注释/BOM → 整段 base64 解包）。
2. **合并去重**：`sites` 按 `(type, api, ext)` 去重；各配置里的相对引用（spider、`api`、`ext`、直播地址）按各自上游地址补成绝对地址，避免换发布地址后失效。
3. **spider 规则**：一份配置只能带一个 spider——取优先级最高且抓取成功的上游为主源，type 3 爬虫站点只收主源配套的；type 0/1/4 采集站点全部合并。
4. **验活**：type 1 用 `ac=list` 探活并验 JSON 结构；type 0 验 XML 结构；type 4 验可达与 JSON；直播源验列表可下载。当天失败只记账，**连续 2 天失败才剔除**（防误杀）。全部上游都抓不到时不覆盖旧文件。
5. **发布**：生成 `out/tvbox.json` 与每日报告 `out/report.txt`，由一个极小的静态服务器在局域网提供。同时生成**精简版** `out/tvbox-lite.json`：按站名类别前缀剔除听书/音频/体育/少儿等非影视站，总数封顶（默认 110），给配置加载慢的盒子用。另外，spider jar、JS 规则、直播列表以及网盘站 `$$$` 复合参数里的 token/分享清单等静态文件会被**镜像**到 `out/assets/` 并改写为相对引用——盒子全部从局域网加载，不再直连慢速的境外文件托管（TVBox 加载配置时要下载校验 spider jar，jar 在境外时光这一步就能超时）。

## 文件

| 文件 | 说明 |
|---|---|
| `tvbox_maintain.py` | 主程序（Python 3 标准库，无第三方依赖） |
| `sources.txt` | 上游清单，一行一个：`名称 \| 网址` 或只写网址，`#` 为注释 |
| `serve.py` | 静态文件服务器（默认 9977 端口，只发布 `out/` 目录） |
| `run.sh` | 包装脚本：`update` 跑一轮维护；`serve-check` 确保发布服务在运行 |
| `state.json` | 运行时生成：各站点连续失败计数 |
| `out/` | 运行时生成：`tvbox.json` 与 `report.txt` |

## 部署（以飞牛 NAS 为例）

```sh
mkdir -p /vol1/1000/tvbox-maintainer
# 把 tvbox_maintain.py / serve.py / run.sh / sources.txt 放进该目录
chmod +x run.sh serve.py tvbox_maintain.py
```

`run.sh` 里的 `BASE` 按实际目录修改。定时任务（crontab）：

```cron
30 4 * * * /vol1/1000/tvbox-maintainer/run.sh update
*/30 * * * * /vol1/1000/tvbox-maintainer/run.sh serve-check
@reboot /vol1/1000/tvbox-maintainer/run.sh serve-check
```

盒子配置地址：`http://<NAS局域网IP>:9977/tvbox.json`（精简版：`http://<NAS局域网IP>:9977/tvbox-lite.json`），每日报告：`http://<NAS局域网IP>:9977/report.txt`。

手动跑一轮：`python3 tvbox_maintain.py`（`sources.txt` 不存在或为空时回退到程序内置种子）；也可以在局域网浏览器打开 `http://<NAS局域网IP>:9977/run` 一键触发（同一时间只跑一轮，重复打开会提示运行中）。

## 已知限制

- type 3 爬虫站点无法逐站探活（需真正运行 spider 才能验），只能保证其上游配置本身可达。
- 个别站点 `ext` 为字典结构时，其内部嵌套的相对路径不会被绝对化（字符串形式的 `$$$` 复合 ext 已支持分段绝对化与镜像）。
- 源站 TLS 证书多有问题，抓取时默认不校验证书（与 TVBox 本体行为一致）。

## 说明

上游配置地址均来自公开渠道，本工具只做收集、合并与存活验证。内容版权归原作者所有，请支持正版。
