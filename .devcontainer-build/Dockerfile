FROM ubuntu:20.04

ARG USER_UID=1000
ARG USER_GID=$USER_UID
ARG USER_NAME=impdev
ARG USER_HOME=/home/${USER_NAME}

# Available Locales are en_US.UTF-8, hu_HU.UTF-8, and hi_IN.UTF-8
ARG LOCALE=en_US.UTF-8

# Specifies the default timezone
ARG TZ="America/Los_Angeles"

ARG IMPALA_BUILD_THREADS=12
ARG IMPALA_CMAKE_VERSION
ARG IMPALA_GCC_VERSION
ARG TOOLCHAIN_PACKAGES_RELATIVE_PATH

ENV DEBIAN_FRONTEND=noninteractive
ENV IMPALA_HOME=${USER_HOME}/impala
ENV IMPALA_CMAKE_VERSION=${IMPALA_CMAKE_VERSION}
ENV IMPALA_GCC_VERSION=${IMPALA_GCC_VERSION}
ENV IMPALA_TOOLCHAIN_PACKAGES_HOME=${IMPALA_HOME}/${TOOLCHAIN_PACKAGES_RELATIVE_PATH}

# Debug UIs for Impala Daemon, Statestore, and Catalog
EXPOSE 25000-25009 25010 25020

# Impala HS2 and HS2-HTTP
EXPOSE 21050-21059 28000-28009

# Postgres for Hive Metastore
EXPOSE 5432

# Ranger UI
EXPOSE 6080

SHELL ["/bin/bash", "-c"]

RUN apt-get update \
    && apt-get install -y tzdata \
    && ln -snf /usr/share/zoneinfo/$TZ /etc/localtime \
    && echo ${TZ} > /etc/timezone \
    && apt-get install -y ca-certificates \
    && update-ca-certificates \
    && apt-get install -y sudo git binfmt-support openjdk-8-jdk psmisc vim locales curl wget openssh-server openssh-client python3-pip emacs nano \
    && locale-gen en_US.UTF-8 hu_HU.UTF-8 hi_IN.UTF-8 \
    && addgroup --gid "${USER_GID}" ${USER_NAME} \
    && adduser --uid "${USER_UID}" --gid "${USER_GID}" --disabled-password --gecos '' --shell /bin/bash --home ${USER_HOME} ${USER_NAME} \
    && echo "${USER_NAME} ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers \
    && echo >> ${USER_HOME}/.bashrc \
    && echo "export USER=${USER_NAME}" >> ${USER_HOME}/.bashrc \
    && echo "export IMPALA_HOME=\${HOME}/impala" >> ${USER_HOME}/.bashrc \
    && echo "export IMPALA_BUILD_THREADS=${IMPALA_BUILD_THREADS}" >> ${USER_HOME}/.bashrc \
    && echo "export IMPALA_LINKER=mold" >> ${USER_HOME}/.bashrc \
    && echo "export LD_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu" >> ${USER_HOME}/.bashrc \
    && echo "export LC_ALL=\"${LOCALE}\"" >> ${USER_HOME}/.bashrc \
    && echo ". \"\${IMPALA_HOME}/bin/impala-config.sh\"" >> ${USER_HOME}/.bashrc \
    && echo ". \"\${IMPALA_HOME}/bin/set-classpath.sh\"" >> ${USER_HOME}/.bashrc \
    && echo "cd \${IMPALA_HOME}" >> ${USER_HOME}/.bashrc \
    && echo >> /etc/sysctl.conf \
    && echo "fs.inotify.max_user_watches=524288" >> /etc/sysctl.conf \
    && sysctl -p

USER ${USER_UID}:${USER_GID}

RUN --mount=type=bind,source=.,target=/tmp/impala \
    mkdir "${USER_HOME}/.ssh" \
    && chmod 700 "${USER_HOME}/.ssh" \
    && echo "github.com ecdsa-sha2-nistp256 AAAAE2VjZHNhLXNoYTItbmlzdHAyNTYAAAAIbmlzdHAyNTYAAABBBEmKSENjQEezOmxkZMy7opKgwFB9nkt5YRrYMjNuG5N87uRgg6CLrbo5wAdT/y6v0mKV0U2w0WZ2YB/++Tpockg=" > "${USER_HOME}/.ssh/known_hosts" \
    && echo "github.com github.infra.cloudera.com ecdsa-sha2-nistp256 AAAAE2VjZHNhLXNoYTItbmlzdHAyNTYAAAAIbmlzdHAyNTYAAABBBIq4UUb+wKlwGnAoh04gzbGf63xOb1Sczuf2G6Sr4WoLm9a6cfEwspS1xi8k9TYyRYJZ90tnhLDCTrS0ZECyX4s=" >> "${USER_HOME}/.ssh/known_hosts" \
    && chmod 600 "${USER_HOME}/.ssh/known_hosts" \
    && cp -R /tmp/impala/ "${IMPALA_HOME}" \
    && cd "${IMPALA_HOME}" \
    && ls -la \
    && curl -Lo .git/hooks/commit-msg https://gerrit.cloudera.org/tools/hooks/commit-msg \
    && chmod 755 .git/hooks/commit-msg \
    && echo "[STATUS] - install virtualenv" \
    && pip3 install virtualenv \
    && echo "[STATUS] - starting system bootstrap" \
    && echo "yes" | ./bin/bootstrap_system.sh \
    && echo "[STATUS] - sourcing impala-config.sh" \
    && . bin/impala-config.sh \
    && echo "[STATUS] - build all" \
    && ./buildall.sh -notests \
    && echo "[STATUS] - starting create-test-configuration.sh" \
    && ./bin/create-test-configuration.sh -create_metastore \
    && rm -f be/build/debug/util/impala-profile-tool \
    && git reflog expire --expire=30.days.ago --all \
    && git gc --aggressive --prune=now \
    && git remote rm origin || true \
    && git remote rm upstream || true \
    && git remote add upstream https://github.com/apache/impala \
    && cp .devcontainer-build/vscode.workspace "${IMPALA_HOME}/impala.code-workspace" \
    && mkdir -p "${USER_HOME}/.local/share/CMakeTools/" \
    && cp .devcontainer-build/cmake-kits.json "${USER_HOME}/.local/share/CMakeTools/cmake-tools-kits.json"

ENTRYPOINT "${IMPALA_HOME}/.devcontainer/entrypoint.sh"
