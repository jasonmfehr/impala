# Apache Impala Devcontainer

## Setup
To develop on Apache Impala using a [devcontainer](https://containers.dev/), a pre-built container image is available on Docker Hub at [apache/impala-dev](https://hub.docker.com/r/apache/impala-dev).  To leverage this container image requires a machine with both Docker and the [devcontainer cli](https://github.com/devcontainers/cli?tab=readme-ov-file#npm-install) installed.

Once those prerequisites are met, pull the container image:
```
docker pull apache/impala-dev:stable
```

Then, retrieve the necessary config files:
```
git clone --single-branch --branch=devcontainer-stable --filter=blob:none --sparse https://github.com/jasonmfehr/impala.git
cd impala
git sparse-checkout set .devcontainer
```

Finally, connect an IDE to the machine and open the `impala` folder (in VSCode, the command is "Dev Containers: Open Folder in Container").  If prompted saying the devcontainer config file has changed should it be reloaded, select No.

## Configuration
The Impala devcontainer is mostly configured and ready to go after startup.  The git `user.name`, `user.email`, and `core.editor` config settings are copied from the local machine into the devcontainer.

After the devcontainer is started, run the command `restart-minicluster` to spin up the necessary dependent services.

To start Impala itself, run `./bin/start-impala-cluster.py`.  Then connect to the cluster with `./bin/impala-shell.sh`.

## Cleanup
The Impala devcontainer leverages a docker volume which is not cleaned up when the devcontainer is destroyed.  Thus, the command `docker volume prune` must be run whenever a devcontainer is destroyed.

Happy developing!
