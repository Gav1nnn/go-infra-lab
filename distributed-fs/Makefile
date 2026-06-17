build:
	@go build -o bin/fs

run: build
	@./bin/fs manager \
		-node node1=http://127.0.0.1:9101 \
		-node node2=http://127.0.0.1:9102 \
		-node node3=http://127.0.0.1:9103

test:
	@go test -v ./...

compose-up:
	@docker compose up --build

compose-down:
	@docker compose down -v

demo: build
	@./scripts/demo.sh
