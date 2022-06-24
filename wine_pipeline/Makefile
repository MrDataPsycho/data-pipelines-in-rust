run_dev_rust:
	RUST_LOG=info cargo run
run_python:
	poetry run python pysrc/main.py
build_release:
	cargo build --release
run_release:
	RUST_LOG=info ./target/release/wine_pipeline
