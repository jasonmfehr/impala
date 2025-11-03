## Helpful Commands When Debugging the Dev Container

```
export DEV_CTR="impaladev4"

docker run -d --name=${DEV_CTR} --entrypoint= jasonmfehr/impaladev:latest /bin/bash -c 'sudo ssh-keygen -A;sudo service ssh start;sudo service postgresql start;sleep infinity'; docker logs -f ${DEV_CTR}
devcontainer set-up --container-id ${DEV_CTR} --config=/home/jfehr/impala/.devcontainer/devcontainer.json

docker exec -it ${DEV_CTR} /bin/bash

docker rm -f ${DEV_CTR}
```