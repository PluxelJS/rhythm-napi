---
packages:
  '@rhythm-app/streamer':
    type: patch
---

## Support ESM package loading

Expose the generated CommonJS runtime through `import`, `require`, and default package conditions so
Node ESM applications and Vite-based test/build pipelines resolve the same native API entry.
