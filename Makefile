.PHONY: up topic seed dry test down

up:
	docker compose up -d
	@sleep 10
	$(MAKE) topic

topic:
	docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists --topic transactions --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092

seed:
	python3 producer/producer.py --rate 50 --duration 60

dry:
	python3 producer/producer.py --dry-run --max-events 10 --rate 1000

test:
	python3 -m pytest -q

down:
	docker compose down -v
