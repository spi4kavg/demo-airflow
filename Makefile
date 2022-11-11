up:
	docker-compose -p demoairflow up

destroy: down clean.containers clean.images

down:
	docker-compose -p demoairflow down

clean.containers:
	docker ps -a | grep demoairflow | awk '{print $$1}' | xargs docker rm -f || true

clean.images:
	docker images -a | grep demoairflow | awk '{print $$1}' | xargs docker rmi -f || true