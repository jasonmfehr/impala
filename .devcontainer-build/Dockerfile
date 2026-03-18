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

ARG BASE_IMAGE
FROM ${BASE_IMAGE}

ARG GIT_REPO
ARG GIT_BRANCH
ARG GIT_HASH
ARG IMPALA_BUILD_THREADS=12
ARG IMPALA_CMAKE_VERSION
ARG IMPALA_GCC_VERSION
ARG DOCKER_BUILD=true
ARG JAVA_VERSION=17

# Source and build metadata labels.
LABEL git.repo="${GIT_REPO}"
LABEL git.branch="${GIT_BRANCH}"
LABEL git.hash="${GIT_HASH}"
LABEL java.version="${JAVA_VERSION}"

# Enable docker specific configurations during Impala setup.
ENV IN_DOCKER=true

# Impala specific environment variables
ENV IMPALA_HOME=${HOME}/impala
ENV IMPALA_CMAKE_VERSION=${IMPALA_CMAKE_VERSION}
ENV IMPALA_GCC_VERSION=${IMPALA_GCC_VERSION}
ENV IMPALA_MINIMAL_DEBUG_INFO=true

# Debug UIs for Impala Daemon, Statestore, and Catalog
EXPOSE 25000-25009 25010 25020

# Impala HS2 and HS2-HTTP
EXPOSE 21050-21059 28000-28009

# Postgres for Hive Metastore
EXPOSE 5432

# Ranger UI
EXPOSE 6080

RUN --mount=type=bind,source=.,target=/tmp/impala \
    cp -R /tmp/impala/ "${IMPALA_HOME}"

WORKDIR ${IMPALA_HOME}
RUN export IMPALA_JDK_VERSION="${JAVA_VERSION}" \
    && echo >> "${HOME}/.bashrc" \
    && cat .devcontainer/bashrc-additions >> "${HOME}/.bashrc" \
    && curl -Lo .git/hooks/commit-msg https://gerrit.cloudera.org/tools/hooks/commit-msg \
    && chmod 755 .git/hooks/commit-msg \
    && echo "[STATUS] - starting system bootstrap" \
    && echo "yes" | ./bin/bootstrap_system.sh \
    && echo "[STATUS] - sourcing impala-config.sh" \
    && . bin/impala-config.sh \
    && echo "[STATUS] - build all" \
    && ./buildall.sh -notests \
    && rm -f be/build/debug/util/impala-profile-tool \
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
    && ./bin/start-impala-cluster.py \
           --catalogd_args="-iceberg_allow_datafiles_in_table_location_only=false" \
    && echo "[STATUS] - loading testdata" \
    && ./bin/load-data.py --workloads=functional-query,tpch \
           --table_formats=text/none,parquet/none/none --exploration_strategy=exhaustive \
    && echo "[STATUS] - shutting down Impala and supporting services" \
    && ./bin/start-impala-cluster.py --kill_only \
    && ./testdata/bin/kill-all.sh \
    && sudo service postgresql stop \
    && sudo service ssh stop \
    && rm -rf "${IMPALA_LOGS_DIR}/cluster/*" \
    && echo "[STATUS] - finished testdata load" \
    && echo "[STATUS] - cleaning/configuring local git repo" \
    && git reflog expire --expire=30.days.ago --all \
    && git gc --aggressive --prune=now \
    && for remote in $(git remote); do git remote rm "${remote}"; done \
    && git remote add upstream https://github.com/apache/impala.git \
    && rm -f "${HOME}/.gitconfig" \
    && echo "[STATUS] - prepopulating Impala shell virtual environment" \
    && ./bin/impala-shell.sh --version \
    && echo "[STATUS] - finished prepopulating Impala shell virtual environment" \
    && cp .devcontainer-build/impala.code-workspace.devcontainer impala.code-workspace

WORKDIR ${HOME}/.local/share/CMakeTools
RUN cp "${IMPALA_HOME}/.devcontainer-build/cmake-kits.json" .

WORKDIR ${IMPALA_HOME}/be/.vscode
RUN cp "${IMPALA_HOME}/.devcontainer-build/c_cpp_properties.json" .

WORKDIR ${HOME}/.ssh
RUN ls | grep -v -e authorized_keys -e config -e known_hosts \
        | xargs -I{} rm -rf {} || true \
    && > authorized_keys \
    && sudo touch /.regen-ssh-keys

WORKDIR ${IMPALA_HOME}
CMD ["${IMPALA_HOME}/.devcontainer/entrypoint.sh"]
