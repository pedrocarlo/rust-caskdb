FILES_TO_LINT=tests/ *.py

run:
	cargo run

test:
	cargo test --verbose

lint:
	cargo clippy --workspace --all-features --all-targets -- -A clippy::all -W clippy::correctness -W clippy::perf -W clippy::suspicious --deny=warnings

coverage:
	cargo llvm-cov --open

build: 
	cargo build
