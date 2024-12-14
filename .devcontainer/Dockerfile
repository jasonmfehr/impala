FROM ubuntu:20.04

ARG USER_UID=1000
ARG USER_GID=$USER_UID
ARG USER_NAME=impdev
ARG USER_HOME=/home/${USER_NAME}

# Available Locales are en_US.UTF-8, hu_HU.UTF-8, and hi_IN.UTF-8
ARG LOCALE=en_US.UTF-8

# Specifies the default timezone
ARG TZ="America/Los_Angeles"

ARG IMPALA_BUILD_THREADS=4

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install -y tzdata \
    && ln -snf /usr/share/zoneinfo/$TZ /etc/localtime \
    && echo ${TZ} > /etc/timezone \
    && apt-get install -y ca-certificates \
    && update-ca-certificates \
    && apt-get install -y ca-certificates-java sudo git openjdk-8-jdk psmisc vim locales \
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
    && echo "if [[ -d \"\${IMPALA_HOME}\" ]]; then cd \${IMPALA_HOME}; fi" >> ${USER_HOME}/.bashrc

USER ${USER_UID}:${USER_GID}

RUN mkdir "${USER_HOME}/impala"
COPY * "${USER_HOME}/impala/"
RUN ls -la "${USER_HOME}/impala"

RUN cd /home/impdev/impala \
   && echo "yes" | ./bin/bootstrap_system.sh \
   && ./buildall.sh -format -skiptests
