build_node:
	touch crates/aptos-build-info/build.rs && cargo build --profile performance -p aptos-node

install_aptos:
	sudo cp target/performance/aptos-node /opt/aptos/bin/

stop_aptos:
	sudo systemctl stop aptos-node.service

start_aptos:
	sudo systemctl start aptos-node.service

update_aptos: stop_aptos install_aptos start_aptos

