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

ARG DEPENDENCY_IMAGE=presto/prestissimo-dependency:centos9
ARG BASE_IMAGE=quay.io/centos/centos:stream9
FROM ${DEPENDENCY_IMAGE} as prestissimo-image

ARG OSNAME=centos
ARG BUILD_TYPE=Release
ARG BUILD_DIR='release'
ARG EXTRA_CMAKE_FLAGS=''
ARG NUM_THREADS=8

ENV PROMPT_ALWAYS_RESPOND=n
ENV BUILD_ROOT=/presto/presto-native-execution
ENV BUILD_BASE_DIR=_build
ENV CCACHE_NOHASHDIR=true
ENV CCACHE_BASEDIR=${BUILD_ROOT}

RUN mkdir -p ${BUILD_ROOT} /runtime-libraries
COPY . ${BUILD_ROOT}
RUN EXTRA_CMAKE_FLAGS="${EXTRA_CMAKE_FLAGS}" \
    NUM_THREADS=${NUM_THREADS} make --directory="${BUILD_ROOT}" cmake-and-build BUILD_TYPE=${BUILD_TYPE} BUILD_DIR=${BUILD_DIR} BUILD_BASE_DIR=${BUILD_BASE_DIR} && \
    ccache -sz -v
RUN !(LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/usr/local/lib:/usr/local/lib64 ldd ${BUILD_ROOT}/${BUILD_BASE_DIR}/${BUILD_DIR}/presto_cpp/main/presto_server  | grep "not found") && \
    LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/usr/local/lib:/usr/local/lib64 ldd ${BUILD_ROOT}/${BUILD_BASE_DIR}/${BUILD_DIR}/presto_cpp/main/presto_server | awk 'NF == 4 { system("cp " $3 " /runtime-libraries") }'

#/////////////////////////////////////////////
#          prestissimo-dev
#//////////////////////////////////////////////

FROM ${DEPENDENCY_IMAGE} as prestissimo-dev

ENV PROMPT_ALWAYS_RESPOND=n
ENV BUILD_ROOT=/presto/presto-native-execution
ENV BUILD_BASE_DIR=_build
ENV CCACHE_NOHASHDIR="true"
ENV CCACHE_BASEDIR=/presto/presto-native-execution

COPY --from=prestissimo-image /root/.cache/ccache /root/.cache/ccache
RUN ccache -sz -v

#/////////////////////////////////////////////
#          prestissimo-runtime
#//////////////////////////////////////////////

FROM ${BASE_IMAGE}

ENV BUILD_BASE_DIR=_build
ARG BUILD_DIR='release'

COPY --chmod=0775 --from=prestissimo-image /presto/presto-native-execution/${BUILD_BASE_DIR}/${BUILD_DIR}/presto_cpp/main/presto_server /usr/bin/
COPY --chmod=0775 --from=prestissimo-image /runtime-libraries/* /usr/lib64/prestissimo-libs/
COPY --chmod=0755 ./etc /opt/presto-server/etc
COPY --chmod=0775 ./entrypoint.sh /opt/entrypoint.sh
RUN echo "/usr/lib64/prestissimo-libs" > /etc/ld.so.conf.d/prestissimo.conf && ldconfig

ENTRYPOINT ["/opt/entrypoint.sh"]
