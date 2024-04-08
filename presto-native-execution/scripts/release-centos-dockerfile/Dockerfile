# prestissimo

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#============================================================
# /////////////////////////////////
#    BUILD TASK DEFINITION
# /////////////////////////////////
#============================================================
ARG BASE_IMAGE=quay.io/centos/centos:stream8
FROM ${BASE_IMAGE} AS build-stage

ARG CPU_TARGET=avx
ARG PRESTODB_REPOSITORY=https://github.com/prestodb/presto
ARG PRESTODB_CHECKOUT=origin/master

USER root:root

ENV CPU_TARGET=${CPU_TARGET}

ENV CC=/opt/rh/gcc-toolset-9/root/bin/gcc
ENV CXX=/opt/rh/gcc-toolset-9/root/bin/g++

ENV PRESTODB_HOME=/opt/presto
ENV DEPENDENCY_DIR=/opt/dependency
ENV INSTALL_PREFIX=/usr/local
ENV CMAKE_PREFIX_PATH=/usr/local
ENV PROMPT_ALWAYS_RESPOND=n

ENV PATH=${DEPENDENCY_DIR}/install/bin:${DEPENDENCY_DIR}/install/bin/hadoop-2.10.1/bin:$PATH

ENV PRESTO_ENABLE_PARQUET=ON
ENV PRESTO_ENABLE_S3=ON
ENV PRESTO_ENABLE_HDFS=ON
ENV PRESTO_ENABLE_REMOTE_FUNCTIONS=ON
ENV PRESTO_ENABLE_TESTING=ON
ENV CLEANUP_BUILD_ARTIFACTS=1

SHELL ["/bin/bash", "-c"]
#-----------------------------------------------------------
#---( START of PrestoDB and Velox dependency tree build scripts
#-----------------------------------------------------------
WORKDIR ${PRESTODB_HOME}
RUN --mount=type=ssh \
    set -exu && \
    dnf -y install openssh-clients git wget unzip make && \
    mkdir -p -m 0600 /root/.ssh && \
    ( \
      ssh-keyscan github.com >> /root/.ssh/known_hosts || true \
    ) && \
    git clone --progress ${PRESTODB_REPOSITORY} "${PRESTODB_HOME}/_repo" && \
    git -C "${PRESTODB_HOME}/_repo" checkout "${PRESTODB_CHECKOUT}" && \
    make --directory="${PRESTODB_HOME}/_repo/presto-native-execution" submodules && \
    mkdir -p "${DEPENDENCY_DIR}/install/bin" \
             "${PRESTODB_HOME}/_repo/presto-native-execution/_build/release" \
             "${PRESTODB_HOME}/catalog" \
             "${PRESTODB_HOME}/tests" && \
    dnf clean all

WORKDIR ${DEPENDENCY_DIR}
RUN --mount=type=ssh \
    set -exu && \
    bash "${PRESTODB_HOME}/_repo/presto-native-execution/scripts/setup-centos.sh" && \
    python3 -m pip install six && \
    set +exu && \
    source /opt/rh/gcc-toolset-9/enable && \
    set -exu && \
    bash "${PRESTODB_HOME}/_repo/presto-native-execution/velox/scripts/setup-adapters.sh" && \
    dnf clean all

#-----------------------------------------------------------
#---( START of PrestoCpp and Velox main build process
#-----------------------------------------------------------
WORKDIR ${PRESTODB_HOME}
RUN --mount=type=ssh \
    source /opt/rh/gcc-toolset-9/enable && \
    set -exu && \
    make --directory="${PRESTODB_HOME}/_repo/presto-native-execution" release \
        PRESTO_ENABLE_PARQUET=${PRESTO_ENABLE_PARQUET} \
        PRESTO_ENABLE_S3=${PRESTO_ENABLE_S3} \
        PRESTO_ENABLE_HDFS=${PRESTO_ENABLE_HDFS} \
        PRESTO_ENABLE_REMOTE_FUNCTIONS=${PRESTO_ENABLE_REMOTE_FUNCTIONS} \
        PRESTO_ENABLE_TESTING=${PRESTO_ENABLE_TESTING} \
        TREAT_WARNINGS_AS_ERRORS=OFF

#-----------------------------------------------------------
#---( START of clean-up artifacts task
#-----------------------------------------------------------
RUN set -exu && \
    [ ${CLEANUP_BUILD_ARTIFACTS} -eq 0 ] || \
    ( \
      rm -rf $(find "${PRESTODB_HOME}/_repo/presto-native-execution/_build/release" | grep -E '\.o$|\.cpp$|\.tcc$|\.h$|CMakeFiles|\.cmake$|\.cs$|\.rb$|\.cc$|\.R$|\.c$|\.hpp$|\.rb$') && \
      rm -rf $(find "${PRESTODB_HOME}/_repo/presto-native-execution/_build/release" | grep -E '\.ninja_deps|\.ninja_log|\.ninja|CMakeCache|\.tcl') \
    ) && \
    ( \
      mv ${PRESTODB_HOME}/_repo/presto-native-execution/_build/release/presto_cpp/main/* ${PRESTODB_HOME}/ && \
      mv ${PRESTODB_HOME}/_repo/presto-native-execution/_build/release/presto_cpp/presto_protocol/tests/presto_protocol_test ${PRESTODB_HOME}/ || true \
    ) && \
    [ ${CLEANUP_BUILD_ARTIFACTS} -eq 0 ] || \
    rm -rf ${PRESTODB_HOME}/_repo/presto-native-execution/_repo /opt/dependency/aws-sdk-cpp /tmp/* && \
    dnf clean all

#============================================================
#---) END of PrestoCpp and Velox main build process
#============================================================
# /////////////////////////////////
#    RUNTIME TASK DEFINITION
# /////////////////////////////////
#============================================================
ARG BASE_IMAGE=quay.io/centos/centos:stream8
FROM ${BASE_IMAGE}

LABEL org.opencontainers.image.title="Prestissimo" \
      org.opencontainers.image.description="Prestissimo Runtime Docker image" \
      org.opencontainers.image.version="1.0.0"

ARG PRESTO_UID=186

SHELL ["/bin/bash", "-c"]

ENV PRESTO_HOME="/opt/presto/"
ENV CPU_TARGET=${CPU_TARGET}
ENV TZ=Europe/Warsaw

WORKDIR /opt/presto/

COPY --chown=${PRESTO_UID} opt/ /opt/
COPY --chown=${PRESTO_UID} --from=build-stage /opt/presto/ /opt/presto/

RUN set -exu && \
    dnf -y install uuid hostname vim git && \
    #---( INFO create a system account (-r) and same name group (-U) with UID and home path at /opt (-d) named presto
    useradd -r -U --uid=${PRESTO_UID} -d /opt presto && \
    chgrp ${PRESTO_UID} /etc/passwd && \
    chown -R ${PRESTO_UID} /opt && \
    chmod -R ug+rw /etc/passwd /opt && \
    chmod ug+x /opt/*.sh /opt/presto/presto_server && \
    rm -rf /root/.cache /tmp && \
    dnf clean all

USER ${PRESTO_UID}

COPY --chown=${PRESTO_UID} etc/ /opt/presto/etc/
COPY --chown=${PRESTO_UID} --from=build-stage /usr/local/lib64/lib* /usr/lib64/lib* /usr/local/lib64/
COPY --chown=${PRESTO_UID} --from=build-stage /usr/local/lib/lib* /usr/local/lib/

ENTRYPOINT [ "/opt/entrypoint.sh" ]

EXPOSE 22 8080

#============================================================
# /////////////////////////////////
#  END OF RUNTIME TASK DEFINITION
# /////////////////////////////////
#============================================================
