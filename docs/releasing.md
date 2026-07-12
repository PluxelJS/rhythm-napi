# npm 发布

npm 主包是 `@rhythm-app/streamer`。原生库由 napi-rs 拆分为四个可选平台包：

- `@rhythm-app/streamer-linux-x64-gnu`
- `@rhythm-app/streamer-win32-x64-msvc`
- `@rhythm-app/streamer-darwin-x64`
- `@rhythm-app/streamer-darwin-arm64`

主包只包含 loader、TypeScript 契约和文档，并通过 `optionalDependencies` 选择当前平台包。
不得把单一平台 `.node` 文件直接发进主包。

## 你需要配置什么

一次性准备以下权限：

- npm 已存在 `rhythm-app` organization，你的 npm 账号对该组织拥有 package publish 权限。
- 你对 GitHub 仓库 `Kokoro-js/rhythm-napi` 拥有 environment、secret 和 Actions 权限。
- GitHub environment 固定命名为 `npm`；workflow 和 trusted publisher 都依赖这个精确名称。

登录 GitHub CLI 并创建 environment：

```sh
gh auth login
gh auth status
gh api --method PUT repos/Kokoro-js/rhythm-napi/environments/npm
```

npm 首次发布需要一个临时 granular access token。在 npm 网站创建 token，限制为
`rhythm-app` organization/package scope，授予 package read/write，并允许它绕过发布 2FA；
不要创建 classic token，也不要把 token 写进仓库。把它直接录入 environment secret：

```sh
gh secret set NPM_TOKEN --env npm --repo Kokoro-js/rhythm-napi
```

## 首次发布 0.1.0

确认 main 分支 CI 成功后，从最新 main 创建并推送首个发布 tag。推送 tag 会自动启动发布：

```sh
git pull --ff-only origin main
git tag -a v0.1.0 -m "Release @rhythm-app/streamer 0.1.0"
git push origin v0.1.0
RUN_ID=$(gh run list --repo Kokoro-js/rhythm-napi --workflow release.yml \
  --limit 1 --json databaseId --jq '.[0].databaseId')
gh run watch "$RUN_ID" --repo Kokoro-js/rhythm-napi --exit-status
```

成功后，npm 上会出现主包和四个平台包。分别打开这五个包的 trusted publisher
设置，并全部填写同一组绑定：

- organization/user：`Kokoro-js`
- repository：`rhythm-napi`
- workflow：`release.yml`
- environment：`npm`

确认五个包都完成绑定后，删除 GitHub secret：

```sh
gh secret delete NPM_TOKEN --env npm --repo Kokoro-js/rhythm-napi
```

同时在 npm 网站撤销临时 token。此后只使用 GitHub OIDC，不再配置 `NPM_TOKEN`。

## 以后发布新版本

以下示例发布 patch 版本。`npm version` 会同步主包和平台包的版本：

```sh
cd crates/music_stream_napi
npm version patch --no-git-tag-version
cd ../..
git add crates/music_stream_napi/package.json \
  crates/music_stream_napi/package-lock.json \
  crates/music_stream_napi/npm
git commit -m "release: @rhythm-app/streamer v$(node -p "require('./crates/music_stream_napi/package.json').version")"
git push origin main
```

打开 GitHub Actions，确认该提交的 `CI` 全部成功，再创建 tag：

```sh
VERSION=$(node -p "require('./crates/music_stream_napi/package.json').version")
git tag -a "v${VERSION}" -m "Release @rhythm-app/streamer ${VERSION}"
git push origin "v${VERSION}"
```

`release.yml` 会并行构建四个 target，组装平台包，通过 GitHub OIDC 发布全部
npm 包和 provenance，并建立 GitHub Release。发布过程无需在本机运行跨平台构建。

tag 与 package version 不一致时 workflow 必须失败，不允许临时修改版本后发布。
