# G1 图传部署分支

`g1-deploy` 基于上游 `v1.5`，供 G1 三路 USB2 相机使用。采用 libuvc，保留单路故障隔离和重连、UVC 关闭超时保护、慢 WebRTC 客户端隔离。仓库中的 `cam_config_server.yaml` 是部署模板。

## 配套版本

`v1.5-g1.3` 搭配采集仓库 `v3.0.1`。从采集仓库安装时固定版本：

```bash
TELEIMAGER_REPO=https://github.com/GuoBinZhuang/teleimager.git \
TELEIMAGER_TAG=v1.5-g1.3 scripts/install_teleimager_1_5.sh
```

相机配置由采集仓库部署，按 VID:PID 同步 `physical_path`。图传由用户单元 `g1-teleimager.service` 管理，日志在 `teleimager_server.log`。排障先用采集仓库的快速自检，空闲时再实测解帧。

上游通用用法见 [README_zh-CN.md](README_zh-CN.md)。
