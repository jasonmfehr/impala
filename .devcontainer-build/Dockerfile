FROM ubuntu:20.04

ARG USER_UID=1000
ARG USER_GID=$USER_UID
ARG USER_NAME=impdev
ARG USER_HOME=/home/${USER_NAME}
ARG USER_IMPALA_DIR=${USER_HOME}/impala

# Available Locales are en_US.UTF-8, hu_HU.UTF-8, and hi_IN.UTF-8
ARG LOCALE=en_US.UTF-8

# Specifies the default timezone
ARG TZ="America/Los_Angeles"

ARG IMPALA_BUILD_THREADS=16

ENV DEBIAN_FRONTEND=noninteractive

SHELL ["/bin/bash", "-c"]

RUN apt-get update \
    && apt-get install -y tzdata \
    && ln -snf /usr/share/zoneinfo/$TZ /etc/localtime \
    && echo ${TZ} > /etc/timezone \
    && apt-get install -y ca-certificates \
    && update-ca-certificates \
    && apt-get install -y sudo git binfmt-support openjdk-8-jdk psmisc vim locales curl wget openssh-server openssh-client python3-pip \
    && locale-gen en_US.UTF-8 hu_HU.UTF-8 hi_IN.UTF-8 \
    && addgroup --gid "${USER_GID}" ${USER_NAME} \
    && adduser --uid "${USER_UID}" --gid "${USER_GID}" --disabled-password --gecos '' --shell /bin/bash --home ${USER_HOME} ${USER_NAME} \
    && echo "${USER_NAME} ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers \
    && echo >> ${USER_HOME}/.bashrc \
    && echo "export USER=${USER_NAME}" >> ${USER_HOME}/.bashrc \
    && echo "export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64" >> ${USER_HOME}/.bashrc \
    && echo "export IMPALA_HOME=\${HOME}/impala" >> ${USER_HOME}/.bashrc \
    && echo "export IMPALA_BUILD_THREADS=${IMPALA_BUILD_THREADS}" >> ${USER_HOME}/.bashrc \
    && echo "export IMPALA_LINKER=mold" >> ${USER_HOME}/.bashrc \
    && echo "export LD_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu" >> ${USER_HOME}/.bashrc \
    && echo "export LC_ALL=\"${LOCALE}\"" >> ${USER_HOME}/.bashrc \
    && echo "if [[ -e \"\${IMPALA_HOME}/bin/set-classpath.sh\" ]]; then source \"\${IMPALA_HOME}/bin/set-classpath.sh\"; fi" >> ${USER_HOME}/.bashrc \
    && echo "if [[ -e \"\${IMPALA_HOME}/bin/impala-config.sh\" ]]; then source \"\${IMPALA_HOME}/bin/impala-config.sh\"; fi" >> ${USER_HOME}/.bashrc \
    && echo "if [[ -d \"\${IMPALA_HOME}\" ]]; then cd \${IMPALA_HOME}; fi" >> ${USER_HOME}/.bashrc \
    && echo >> /etc/sysctl.conf \
    && echo "fs.inotify.max_user_watches=524288" >> /etc/sysctl.conf \
    && sysctl -p

USER ${USER_UID}:${USER_GID}

RUN --mount=type=bind,source=.,target=/tmp/impala \
    cp -R /tmp/impala/ "${USER_IMPALA_DIR}" \
    && ls -la "${USER_IMPALA_DIR}" \
    && mkdir "${USER_HOME}/.ssh" \
    && chmod 700 "${USER_HOME}/.ssh" \
    && echo "github.com ecdsa-sha2-nistp256 AAAAE2VjZHNhLXNoYTItbmlzdHAyNTYAAAAIbmlzdHAyNTYAAABBBEmKSENjQEezOmxkZMy7opKgwFB9nkt5YRrYMjNuG5N87uRgg6CLrbo5wAdT/y6v0mKV0U2w0WZ2YB/++Tpockg=" > ${USER_HOME}/.ssh/known_hosts \
    && curl -Lo "${USER_IMPALA_DIR}/.git/hooks/commit-msg" https://gerrit.cloudera.org/tools/hooks/commit-msg \
    && chmod 755 "${USER_IMPALA_DIR}/.git/hooks/commit-msg" \
    && cd ${USER_IMPALA_DIR} \
    && echo "[STATUS] - install virtualenv" \
    && pip3 install virtualenv \
    && echo "[STATUS] - starting system bootstrap" \
    && echo "yes" | ./bin/bootstrap_system.sh \
    && echo "[STATUS] - sourcing impala-config.sh" \
    && source "${USER_IMPALA_DIR}/bin/impala-config.sh" \
    && echo "[STATUS] - build all" \
    && ./buildall.sh -noclean -skiptests \
    && echo "[STATUS] - starting create-test-configuration.sh" \
    && ./bin/create-test-configuration.sh -create_metastore \
    && git remote rm origin || true \
    && git remote rm upstream || true \
    && git remote add upstream https://github.com/apache/impala \
    && cp .devcontainer-build/vscode.workspace impala.code-workspace \
    && mkdir .vscode \
    && cp .devcontainer-build/vscode.settings.json .vscode/settings.json

ENTRYPOINT ["/home/impdev/impala/.devcontainer-build/entrypoint.sh"]

