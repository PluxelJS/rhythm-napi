# 文档索引

这里的文档描述当前系统，而不是记录它如何演变到当前状态。设计事实只在一个地方定义，
其他文档通过链接引用，避免同一语义出现多个版本。

## 推荐阅读顺序

1. [architecture.md](architecture.md)：先理解系统解决什么问题、边界在哪里、谁拥有什么。
2. [runtime.md](runtime.md)：再理解一次播放如何启动、流转、暂停、失败和结束。
3. [latency.md](latency.md)：理解零起点 URL 如何边下载边播放，以及首包的真实退化边界。
4. [implementation.md](implementation.md)：最后进入 source、codec、queue 和 RTP 的实现约束。

面向 Node.js 宿主的接入者直接阅读 [user-doc.md](user-doc.md)。它按业务工作流说明如何组织
source、current/next、事件、错误恢复和 shutdown，不复制生成的 TypeScript API 清单。

## 参考文档

- [testing.md](testing.md)：必须保护的不变量、验证层次和性能判断方法。
- [dependencies.md](dependencies.md)：依赖边界以及为什么选择这些依赖。
- [status.md](status.md)：当前能力边界、尚需产品输入的扩展和演进准入条件。
- [releasing.md](releasing.md)：napi-rs 平台包、GitHub Actions、npm OIDC 和版本发布契约。

## 文档维护规则

- 文档只写当前模型、设计理由和可验证契约，不保留阶段计划、旧方案或完成记录。
- 架构文档定义所有权；runtime 文档定义时序；implementation 文档定义实现约束。
- 尚未实现的设想只能进入 `status.md`，不得混写成当前行为。
- 配置字段和 TypeScript 形状以构建生成的 `index.d.ts` 为准；使用文档只解释组合方式和语义。
- 修改 pause、seek、source、错误或 RTP 时钟语义时，代码、测试和对应文档必须在同一改动中更新。
