build_node:
	cargo build --release -p aptos-node

build_grpc_manager:
	cargo build --release  -p aptos-indexer-grpc-manager

build_data_service:
	cargo build --release -p aptos-indexer-grpc-data-service-v2
