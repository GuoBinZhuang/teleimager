# g1-deploy 分支

这个分支只放 **Unitree G1 三相机部署配置**，基于上游 `v1.5`，不改动任何代码。

- `cam_config_server.yaml`：三路 USB 相机配置（头部双目 + 双腕），`type: uvc`
- `.gitignore`：补上 `*.log`，避免运行日志被提交

## 使用

```bash
git clone -b g1-deploy https://github.com/GuoBinZhuang/teleimager.git ~/gb_workspace/teleimager-1.5
# 或从已有 v1.5 目录取配置：
git -C <repo> show g1-deploy:cam_config_server.yaml > cam_config_server.yaml
```

`physical_path` 是 USB 物理端口，换机器/换口后由
[g1-mcap-capture](https://github.com/GuoBinZhuang/g1-mcap-capture) 的
`scripts/sync_camera_paths.py` 按 VID:PID 自动纠正，无需手改。

> 代码与上游保持一致；升级请切回 `main` 或对应 tag，不要在这个分支上改源码。
