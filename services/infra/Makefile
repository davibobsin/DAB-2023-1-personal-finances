run:
	docker-compose --profile all up

clean:
	docker stop redpanda-0
	docker rm redpanda-0
	docker volume rm infra_redpanda-0
	docker stop postgres
	docker rm postgres

create-topics:
	rpk topic create "nubank.card.entry" --brokers=localhost:19092
	rpk topic create "nubank.account.entry" --brokers=localhost:19092
	rpk topic create "itau.account.entry" --brokers=localhost:19092
	rpk topic create "processed.account.entry" --brokers=localhost:19092
	rpk topic create "processed.card.entry" --brokers=localhost:19092
	rpk topic create "processed.error" --brokers=localhost:19092
	rpk topic create "processed.fraud" --brokers=localhost:19092

