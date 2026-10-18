import { defineConfig } from 'vite-plus'

const nativeInputs = [
	{ auto: true },
	{ pattern: '!**/node_modules/.vite/**', base: 'workspace' as const },
	{ pattern: '!**/node_modules/.vitest-cache/**', base: 'workspace' as const },
	{ pattern: '../../Cargo.toml', base: 'package' as const },
	{ pattern: '../../Cargo.lock', base: 'package' as const },
	{ pattern: '../../mise.toml', base: 'package' as const },
	{ pattern: 'mise.toml', base: 'workspace' as const },
	{ pattern: '../music_stream/**', base: 'package' as const },
]

export default defineConfig({
	run: {
		tasks: {
			compile: {
				command: 'napi build --platform --release',
				cache: {
					input: nativeInputs,
					output: ['index.js', 'index.d.ts', '*.node'],
					env: ['RUSTUP_TOOLCHAIN', 'CARGO_BUILD_TARGET', 'RUSTFLAGS', 'CARGO_TARGET_DIR'],
					untrackedEnv: ['RUSTUP_HOME', 'CARGO_HOME', 'CARGO_BUILD_JOBS'],
				},
			},
			'compile-debug': { command: 'napi build --platform', cache: false },
			'soak-runtime': {
				command: 'node scripts/soak.mjs',
				dependsOn: ['compile'],
				cache: false,
			},
			'check-bindings': {
				command: 'tsc --noEmit && vp test run --maxWorkers=2 --config vitest.config.mts',
				cache: {
					input: [...nativeInputs, '*.node', 'index.js', 'index.d.ts'],
					output: [],
					env: ['RUSTUP_TOOLCHAIN', 'CARGO_BUILD_TARGET', 'RUSTFLAGS', 'CARGO_TARGET_DIR'],
					untrackedEnv: ['RUSTUP_HOME', 'CARGO_HOME', 'CARGO_BUILD_JOBS'],
				},
			},
			'check-tests': {
				command: 'tsc --noEmit && vp test run --maxWorkers=2 --config vitest.config.mts',
				dependsOn: ['compile'],
				cache: {
					input: nativeInputs,
					output: [],
					env: ['RUSTUP_TOOLCHAIN', 'CARGO_BUILD_TARGET', 'RUSTFLAGS', 'CARGO_TARGET_DIR'],
					untrackedEnv: ['RUSTUP_HOME', 'CARGO_HOME', 'CARGO_BUILD_JOBS'],
				},
			},
		},
	},
})
