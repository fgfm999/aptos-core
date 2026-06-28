build_node:
	cargo build --profile performance -p aptos-node

build_grpc_manager:
	cargo build --profile performance -p aptos-indexer-grpc-manager

build_data_service:
	cargo build --profile performance -p aptos-indexer-grpc-data-service-v2

install_aptos:
	sudo systemctl start aptos-node.service

stop_aptos:
	sudo systemctl stop aptos-node.service

start_aptos:
	sudo systemctl start aptos-node.service
