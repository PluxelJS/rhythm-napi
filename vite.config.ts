import { defineConfig } from 'vite-plus'

const rustCache = {
	input: [
		'Cargo.toml',
		'Cargo.lock',
		'mise.toml',
		'crates/**/Cargo.toml',
		'crates/**/src/**',
		'crates/**/tests/**',
		'crates/**/benches/**',
		'crates/**/build.rs',
		'crates/**/testdata/**',
	],
	output: [],
	env: ['RUSTUP_TOOLCHAIN', 'CARGO_BUILD_TARGET', 'RUSTFLAGS', 'CARGO_TARGET_DIR'],
	untrackedEnv: ['RUSTUP_HOME', 'CARGO_HOME', 'CARGO_BUILD_JOBS'],
}

export default defineConfig({
	lint: {
		options: { reportUnusedDisableDirectives: 'error' },
		ignorePatterns: [
			'target/**',
			'**/node_modules/**',
			'**/index.js',
			'**/index.d.ts',
			'**/dist/**',
		],
	},
	fmt: {
		useTabs: true,
		tabWidth: 2,
		printWidth: 100,
		semi: false,
		singleQuote: true,
		sortPackageJson: false,
		ignorePatterns: [
			'target/**',
			'**/node_modules/**',
			'**/index.js',
			'**/index.d.ts',
			'**/dist/**',
			'**/*.node',
		],
	},
	run: {
		cache: { scripts: false, tasks: true },
		tasks: {
			'check-tools': { command: 'vp check', cache: false },
			'check-rust-format': { command: 'cargo fmt --all -- --check', cache: false },
			'check-rust-tests': { command: 'cargo test --workspace --all-targets', cache: rustCache },
			'check-rust-lint': {
				command: 'cargo clippy --workspace --all-targets -- -D warnings',
				cache: rustCache,
			},
			'check-rust-docs': { command: 'cargo doc --workspace --no-deps', cache: false },
			'workspace:test': {
				command: [],
				dependsOn: ['check-rust-tests', '@rhythm-app/streamer#check-tests'],
			},
			'workspace:verify': {
				command: [],
				dependsOn: [
					'check-tools',
					'check-rust-format',
					'workspace:test',
					'check-rust-lint',
					'check-rust-docs',
				],
				cache: false,
			},
		},
	},
})
