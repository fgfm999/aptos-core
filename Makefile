build_node:
	cargo build --profile performance -p aptos-node

build_grpc_manager:
	cargo build --profile performance -p aptos-indexer-grpc-manager

build_data_service:
	cargo build --profile performance -p aptos-indexer-grpc-data-service-v2
