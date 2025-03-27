airflow_up:
	docker compose -f airflow-docker-compose.yaml up -d --build
airflow_down:
	docker compose -f airflow-docker-compose.yaml down

batch_up:
	docker compose -f docker-compose.yaml up -d
batch_down:
	docker compose -f docker-compose.yaml down

run_all:
	docker compose -f docker-compose.yaml up -d
	docker compose -f airflow-docker-compose.yaml up -d --build

stop_all:
	docker compose -f docker-compose.yaml down
	docker compose -f airflow-docker-compose.yaml down

remove_all:
	docker compose -f airflow-docker-compose.yaml down --rmi all;
	docker compose -f docker-compose.yaml down --rmi all;