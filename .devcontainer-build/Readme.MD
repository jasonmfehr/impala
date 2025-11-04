## Helpful Commands When Debugging the Dev Container

```
export DEV_CTR=impaladev

docker volume create vol-${DEV_CTR} && docker run -d --name=${DEV_CTR} --entrypoint= --volume ${DEV_CTR}:/home/impdev/impala jasonmfehr/impaladev:latest /bin/bash -c 'sudo ssh-keygen -A;sudo service ssh start;sudo service postgresql start;sleep infinity'; docker logs -f ${DEV_CTR}

docker exec -it ${DEV_CTR} /bin/bash

docker rm -f ${DEV_CTR}

# Does Not Appear to be Needed
# devcontainer set-up --container-id ${DEV_CTR} --config=/home/jfehr/impala/.devcontainer/devcontainer.json
```