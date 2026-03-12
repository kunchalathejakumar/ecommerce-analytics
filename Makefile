up:
	docker-compose -f docker/docker-compose.yml --env-file .env up -d

down:
	docker-compose -f docker/docker-compose.yml --env-file .env down

logs:
	docker-compose -f docker/docker-compose.yml --env-file .env logs -f