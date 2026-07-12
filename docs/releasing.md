# 发布契约

npm 主包是 `@rhythm-app/streamer`。原生库由 napi-rs 拆分为四个可选平台包：

- `@rhythm-app/streamer-linux-x64-gnu`
- `@rhythm-app/streamer-win32-x64-msvc`
- `@rhythm-app/streamer-darwin-x64`
- `@rhythm-app/streamer-darwin-arm64`

主包只包含 loader、TypeScript 契约和文档，并通过 `optionalDependencies` 选择当前平台包。
不得把单一平台 `.node` 文件直接发进主包。

## GitHub 与 npm 一次性设置

先登录 GitHub CLI 并创建发布 environment：

```sh
gh auth login
gh auth status
gh api --method PUT repos/Kokoro-js/rhythm-napi/environments/npm
```

npm trusted publisher 对主包和四个平台包都使用同一组绑定：

   - organization/user: `Kokoro-js`
   - repository: `rhythm-napi`
   - workflow: `release.yml`
   - environment: `npm`

### 首次 bootstrap

trusted publisher 通常要求包已存在。在 npm 网站创建一个仅限 `@rhythm-app` 的短期 granular
publish token，然后执行：

```sh
gh secret set NPM_TOKEN --env npm --repo Kokoro-js/rhythm-napi
gh workflow run release.yml --repo Kokoro-js/rhythm-napi -f tag=v0.1.0
RUN_ID=$(gh run list --repo Kokoro-js/rhythm-napi --workflow release.yml \
  --limit 1 --json databaseId --jq '.[0].databaseId')
gh run watch "$RUN_ID" --repo Kokoro-js/rhythm-napi --exit-status
```

首发成功后，在 npm 网站为五个包配置上述 trusted publisher，再立即执行：

```sh
gh secret delete NPM_TOKEN --env npm --repo Kokoro-js/rhythm-napi
```

同时在 npm 网站撤销该临时 token。之后的正常发布只使用 GitHub OIDC，不保存 npm token。

## 发布方式

例如发布一个 patch 版本：

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

等 main CI 通过后再创建 tag：

```sh
VERSION=$(node -p "require('./crates/music_stream_napi/package.json').version")
git tag -a "v${VERSION}" -m "Release @rhythm-app/streamer ${VERSION}"
git push origin "v${VERSION}"
```

`release.yml` 会并行构建四个 target，使用 `napi artifacts` 组装平台包，
   通过 GitHub OIDC 发布所有 npm 包并上传 provenance，最后建立 GitHub Release。

tag 与 package version 不一致时 workflow 必须失败，不允许临时修改版本后发布。
