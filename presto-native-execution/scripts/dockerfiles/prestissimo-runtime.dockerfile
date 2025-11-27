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
ARG EXTRA_CMAKE_FLAGS=''
ARG NUM_THREADS=8
ARG CUDA_ARCHITECTURES=70

ENV PROMPT_ALWAYS_RESPOND=n
ENV BUILD_BASE_DIR=_build
ENV BUILD_DIR=""

WORKDIR /prestissimo

# Step 1: Copy build system files first (rarely change)
COPY Makefile CMakeLists.txt ./

# Step 2: Copy velox submodule (large but changes infrequently)
COPY velox/ velox/

# Step 3: Copy presto_cpp directory structure
COPY presto_cpp/ presto_cpp/

# Step 4: Run CMake configure (cacheable)
RUN --mount=type=cache,target=/root/.ccache,sharing=locked \
    --mount=type=cache,target=/prestissimo/${BUILD_BASE_DIR},sharing=locked \
    cmake -B ${BUILD_BASE_DIR}/${BUILD_DIR} \
          -DCMAKE_BUILD_TYPE=${BUILD_TYPE} \
          ${EXTRA_CMAKE_FLAGS}

## Step 5: Copy remaining files (scripts, configs, etc.)
#COPY etc/ etc/
#COPY etc_sidecar/ etc_sidecar/
#COPY entrypoint.sh entrypoint.sh
#
#COPY scripts/ scripts/
#COPY docs/ docs/

# Step 6: Incremental build
RUN --mount=type=cache,target=/root/.ccache,sharing=locked \
    --mount=type=cache,target=/prestissimo/${BUILD_BASE_DIR},sharing=locked \
    cmake --build ${BUILD_BASE_DIR}/${BUILD_DIR} -j ${NUM_THREADS} && \
    ccache -sz -v

# Step 7: Extract runtime dependencies
RUN mkdir -p /runtime-libraries && \
    !(LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/usr/local/lib:/usr/local/lib64 ldd /prestissimo/${BUILD_BASE_DIR}/${BUILD_DIR}/presto_cpp/main/presto_server | grep "not found") && \
    LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/usr/local/lib:/usr/local/lib64 ldd /prestissimo/${BUILD_BASE_DIR}/${BUILD_DIR}/presto_cpp/main/presto_server | awk 'NF == 4 { system("cp " $3 " /runtime-libraries") }' \

#/////////////////////////////////////////////
#          prestissimo-runtime
#//////////////////////////////////////////////

FROM ${BASE_IMAGE}

ENV BUILD_BASE_DIR=_build
ENV BUILD_DIR=""

COPY --chmod=0775 --from=prestissimo-image /prestissimo/${BUILD_BASE_DIR}/${BUILD_DIR}/presto_cpp/main/presto_server /usr/bin/
COPY --chmod=0775 --from=prestissimo-image /runtime-libraries/* /usr/lib64/prestissimo-libs/
COPY --chmod=0755 ./etc /opt/presto-server/etc
COPY --chmod=0775 ./entrypoint.sh /opt/entrypoint.sh
RUN echo "/usr/lib64/prestissimo-libs" > /etc/ld.so.conf.d/prestissimo.conf && ldconfig

ENTRYPOINT ["/opt/entrypoint.sh"]