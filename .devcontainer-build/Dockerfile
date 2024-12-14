# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

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
ARG DOCKER_BUILD=true
ARG JAVA_VERSION=8

# General environment variables
ENV DEBIAN_FRONTEND=noninteractive
ENV IN_DOCKER=true

# Impala specific environment variables
ENV IMPALA_HOME=${USER_HOME}/impala
ENV IMPALA_CMAKE_VERSION=${IMPALA_CMAKE_VERSION}
ENV IMPALA_GCC_VERSION=${IMPALA_GCC_VERSION}
ENV IMPALA_TOOLCHAIN_PACKAGES_HOME=${IMPALA_HOME}/${TOOLCHAIN_PACKAGES_RELATIVE_PATH}
ENV IMPALA_MINIMAL_DEBUG_INFO=true

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
    && apt-get install -y ca-certificates \
    && update-ca-certificates \
    && apt-get install -y tzdata \
    && ln -snf /usr/share/zoneinfo/$TZ /etc/localtime \
    && echo ${TZ} > /etc/timezone \
    && apt-get install -y sudo git binfmt-support openjdk-8-jdk openjdk-21-jdk psmisc vim emacs nano locales curl wget openssh-server openssh-client python3-pip dnsutils shellcheck bash-completion \
    && locale-gen en_US.UTF-8 hu_HU.UTF-8 hi_IN.UTF-8 \
    && update-java-alternatives --list | grep "1.${JAVA_VERSION}" | rev | cut -d" " -f1 | rev | xargs update-java-alternatives --set \
    && update-java-alternatives --list | grep "1.8" | rev | cut -d" " -f1 | rev | xargs -I{} ln -s {} /usr/lib/jvm/java-8-openjdk \
    && update-java-alternatives --list | grep "1.21" | rev | cut -d" " -f1 | rev | xargs -I{} ln -s {} /usr/lib/jvm/java-21-openjdk \
    && addgroup --gid "${USER_GID}" ${USER_NAME} \
    && adduser --uid "${USER_UID}" --gid "${USER_GID}" --disabled-password --gecos '' --shell /bin/bash --home ${USER_HOME} ${USER_NAME} \
    && echo "${USER_NAME} ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers \
    && echo >> ${USER_HOME}/.bashrc \
    && echo "export USER=${USER_NAME}" >> ${USER_HOME}/.bashrc \
    && echo "export IMPALA_HOME=\${HOME}/impala" >> ${USER_HOME}/.bashrc \
    && echo "export IMPALA_LINKER=mold" >> ${USER_HOME}/.bashrc \
    && echo "export LD_LIBRARY_PATH=/usr/lib/$(uname -p)-linux-gnu" >> ${USER_HOME}/.bashrc \
    && echo "export LC_ALL=\"${LOCALE}\"" >> ${USER_HOME}/.bashrc \
    && echo >> ${USER_HOME}/.bashrc \
    && echo "get_current_branch() { local ref; ref=\$(git symbolic-ref --short HEAD 2>/dev/null); [ -n \"\$ref\" ] && echo \" [\$ref]\"; }" >> ${USER_HOME}/.bashrc \
    && echo "export PS1='\\u:\\w\$(get_current_branch)\\\$ '" >> ${USER_HOME}/.bashrc \
    && echo >> ${USER_HOME}/.bashrc \
    && echo "alias ga='git add'" >> ${USER_HOME}/.bashrc \
    && echo "alias gb='git branch'" >> ${USER_HOME}/.bashrc \
    && echo "alias gba='git branch -a'" >> ${USER_HOME}/.bashrc \
    && echo "alias gc='git commit -v'" >> ${USER_HOME}/.bashrc \
    && echo "alias gd='git diff'" >> ${USER_HOME}/.bashrc \
    && echo "alias grv='git remote -v'" >> ${USER_HOME}/.bashrc \
    && echo "alias gs='git status'" >> ${USER_HOME}/.bashrc \
    && echo "alias critique='pushd \"\${IMPALA_HOME}\" && ./bin/jenkins/critique-gerrit-review.py --dryrun; popd'" >> ${USER_HOME}/.bashrc \
    && echo "alias restart-minicluster='pushd \"\${IMPALA_HOME}\" && ./testdata/bin/run-all.sh; popd'" >> ${USER_HOME}/.bashrc \
    && echo "alias setup-asf-gerrit-remote='GIT_CFG_USER=\$(git config user.name); read -rp \"Enter Github.com Username, press return to use default of \\\"\${GIT_CFG_USER}\\\"> \" GIT_USER; if [[ -z \"\${GIT_USER}\" ]]; then GIT_USER=\"\${GIT_CFG_USER}\"; fi; pushd \"\${IMPALA_HOME}\"; git remote add asf-gerrit \"ssh://\${GIT_USER}@gerrit.cloudera.org:29418/Impala-ASF\" 2>/dev/null; git remote set-url asf-gerrit \"ssh://\${GIT_USER}@gerrit.cloudera.org:29418/Impala-ASF\"; echo; echo \"asf-gerrit remote:\"; git remote -v | grep asf-gerrit; echo; popd; echo; echo \"Add this SSH public key to Gerrit: \"; cat \"\${HOME}/.ssh/id_rsa.pub\"; echo'" >> ${USER_HOME}/.bashrc \
    && echo "alias push-asf-gerrit-draft='git push asf-gerrit HEAD:refs/drafts/master'" >> ${USER_HOME}/.bashrc \
    && echo "alias push-asf-gerrit-publish='git push asf-gerrit HEAD:refs/for/master'" >> ${USER_HOME}/.bashrc \
    && echo >> ${USER_HOME}/.bashrc \
    && echo "unset IMPALA_TOOLCHAIN_COMMIT_HASH" >> ${USER_HOME}/.bashrc \
    && echo ". \"\${IMPALA_HOME}/bin/impala-config.sh\"" >> ${USER_HOME}/.bashrc \
    && echo ". \"\${IMPALA_HOME}/bin/set-classpath.sh\"" >> ${USER_HOME}/.bashrc \
    && echo "cd \${IMPALA_HOME}" >> ${USER_HOME}/.bashrc \
    && echo >> /etc/sysctl.conf \
    && echo "fs.inotify.max_user_watches=524288" >> /etc/sysctl.conf \
    && sysctl -p

USER ${USER_UID}:${USER_GID}

RUN --mount=type=bind,source=.,target=/tmp/impala \
    set -x \
    && mkdir "${USER_HOME}/.ssh" \
    && chmod 700 "${USER_HOME}/.ssh" \
    && echo "github.com ecdsa-sha2-nistp256 AAAAE2VjZHNhLXNoYTItbmlzdHAyNTYAAAAIbmlzdHAyNTYAAABBBEmKSENjQEezOmxkZMy7opKgwFB9nkt5YRrYMjNuG5N87uRgg6CLrbo5wAdT/y6v0mKV0U2w0WZ2YB/++Tpockg=" > "${USER_HOME}/.ssh/known_hosts" \
    && ssh-keyscan -p 29418 gerrit.cloudera.org >> ~/.ssh/known_hosts >> "${USER_HOME}/.ssh/known_hosts" \
    && chmod 600 "${USER_HOME}/.ssh/known_hosts" \
    && cp -R /tmp/impala/ "${IMPALA_HOME}" \
    && cd "${IMPALA_HOME}" \
    && curl -Lo .git/hooks/commit-msg https://gerrit.cloudera.org/tools/hooks/commit-msg \
    && chmod 755 .git/hooks/commit-msg \
    && pip3 install virtualenv \
    && echo "[STATUS] - starting system bootstrap" \
    && echo "yes" | ./bin/bootstrap_system.sh \
    && echo "[STATUS] - sourcing impala-config.sh" \
    && . bin/impala-config.sh \
    && echo "[STATUS] - build all" \
    && ./buildall.sh -notests \
    && echo "[STATUS] - starting create-test-configuration.sh" \
    && ./bin/create-test-configuration.sh -create_metastore \
    && echo "[STATUS] - starting create_testdata.sh" \
    && ./bin/create_testdata.sh \
    && echo "[STATUS] - starting testdata load" \
    && sudo ssh-keygen -A \
    && sudo service ssh start \
    && sudo service postgresql start \
    && echo "[STATUS] - starting minicluster" \
    && . bin/impala-config.sh \
    && . bin/set-classpath.sh \
    && ./testdata/bin/run-all.sh \
    && echo "[STATUS] - starting Impala" \
    && ./bin/start-impala-cluster.py --catalogd_args="-iceberg_allow_datafiles_in_table_location_only=false" \
    && echo "[STATUS] - loading testdata" \
    && ./bin/load-data.py --workloads=functional-query,tpch --table_formats=text/none,parquet/none/none --exploration_strategy=exhaustive \
    && echo "[STATUS] - shutting down Impala and supporting services" \
    && ./bin/start-impala-cluster.py --kill_only \
    && ./testdata/bin/kill-all.sh \
    && sudo service postgresql stop \
    && sudo service ssh stop \
    && echo "[STATUS] - finished testdata load" \
    && rm -f be/build/debug/util/impala-profile-tool \
    && git reflog expire --expire=30.days.ago --all \
    && git gc --aggressive --prune=now \
    && git remote rm origin || true \
    && git remote rm upstream || true \
    && git remote add upstream https://github.com/apache/impala \
    && rm -f ${USER_HOME}/.gitconfig \
    && cp .devcontainer-build/vscode.workspace "${IMPALA_HOME}/impala.code-workspace" \
    && mkdir -p "${USER_HOME}/.local/share/CMakeTools/" \
    && cp .devcontainer-build/cmake-kits.json "${USER_HOME}/.local/share/CMakeTools/cmake-tools-kits.json" \
    && mkdir "${IMPALA_HOME}/be/.vscode" \
    && cp .devcontainer-build/c_cpp_properties.json "${IMPALA_HOME}/be/.vscode"

CMD ["${IMPALA_HOME}/.devcontainer/entrypoint.sh"]
