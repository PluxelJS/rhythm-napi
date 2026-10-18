import { defineConfig } from 'vite-plus'

export default defineConfig({
	test: {
		include: ['test/**/*.test.ts'],
		testTimeout: 5_000,
		hookTimeout: 5_000,
	},
})
