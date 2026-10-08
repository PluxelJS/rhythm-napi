import { defineConfig } from 'pncat'

export default defineConfig({
  saveExact: false,
  depFields: { peerDependencies: false },
  catalogRules: [
    { name: 'test', match: ['vitest'], priority: 10 },
    { name: 'tooling', match: ['@napi-rs/cli', '@types/node', 'typescript', 'pncat'], priority: 20 },
  ],
})
