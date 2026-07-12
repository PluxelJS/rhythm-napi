# 发布契约

npm 主包是 `@rhythm-app/streamer`。原生库由 napi-rs 拆分为四个可选平台包：

- `@rhythm-app/streamer-linux-x64-gnu`
- `@rhythm-app/streamer-win32-x64-msvc`
- `@rhythm-app/streamer-darwin-x64`
- `@rhythm-app/streamer-darwin-arm64`

主包只包含 loader、TypeScript 契约和文档，并通过 `optionalDependencies` 选择当前平台包。
不得把单一平台 `.node` 文件直接发进主包。

## GitHub 与 npm 一次性设置

1. GitHub repository 使用 `Kokoro-js/rhythm-napi`，默认分支为 `main`。
2. 在 GitHub repository 中建立名为 `npm` 的 environment，可选配置发布审批人。
3. npm trusted publisher 对主包和四个平台包都绑定：
   - organization/user: `Kokoro-js`
   - repository: `rhythm-napi`
   - workflow: `release.yml`
   - environment: `npm`
4. trusted publisher 需要包已存在时，首次 bootstrap 可在 GitHub `npm` environment 中临时放入一个
   granular `NPM_TOKEN`，让同一 release workflow 创建五个包。绑定 OIDC 后立即撤销并删除
   secret；正常发布不依赖 token。

## 发布方式

1. 通过 `npm version` 更新 `crates/music_stream_napi/package.json` 与 lockfile，并同步调整不发布的
   `crates/music_stream_napi/Cargo.toml` crate version。
2. 执行 `npm run create:npm` 和 `npx napi version`，提交版本变更。
3. 在通过 CI 的 commit 上创建与 package version 一致的 tag，例如 `v0.1.0`。
4. push tag。`release.yml` 会并行构建四个 target，使用 `napi artifacts` 组装平台包，
   通过 GitHub OIDC 发布所有 npm 包并上传 provenance，最后建立 GitHub Release。

tag 与 package version 不一致时 workflow 必须失败，不允许临时修改版本后发布。
