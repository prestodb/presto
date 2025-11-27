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

# NOTE: Unlike the upstream version, we don't build prestissimo inside Docker.
# The binary is pre-built by the CI workflow, then copied into this image.
# This stage only extracts the runtime library dependencies.
ARG BUILDER_IMAGE
ARG BASE_IMAGE=quay.io/centos/centos:stream9
FROM ${BUILDER_IMAGE} as prestissimo-image

ENV BUILD_BASE_DIR=_build
ENV BUILD_DIR=release

# Copy the pre-built binary from the build context
COPY presto-native-execution/${BUILD_BASE_DIR}/${BUILD_DIR}/presto_cpp/main/presto_server /tmp/presto_server

RUN mkdir -p /runtime-libraries && \
    !(LD_LIBRARY_PATH=/usr/local/lib:/usr/local/lib64 ldd /tmp/presto_server | grep "not found") && \
    LD_LIBRARY_PATH=/usr/local/lib:/usr/local/lib64 ldd /tmp/presto_server | awk 'NF == 4 { system("cp " $3 " /runtime-libraries") }'

#/////////////////////////////////////////////
#          prestissimo-runtime
#//////////////////////////////////////////////

FROM ${BASE_IMAGE}

# NOTE:
# - We need `ca-certificates` to support reads from signed S3 URLs.
# - We need `tzdata` as a temporary workaround for https://github.com/prestodb/presto/issues/25531
RUN dnf install -y \
    ca-certificates \
    tzdata \
    && dnf clean all \
    && rm -rf /var/cache/dnf

COPY --chmod=0775 --from=prestissimo-image /tmp/presto_server /usr/bin/
COPY --chmod=0775 --from=prestissimo-image /runtime-libraries/* /usr/lib64/prestissimo-libs/
COPY --chmod=0755 ./presto-native-execution/etc /opt/presto-server/etc
COPY --chmod=0775 ./presto-native-execution/entrypoint.sh /opt/entrypoint.sh
RUN echo "/usr/lib64/prestissimo-libs" > /etc/ld.so.conf.d/prestissimo.conf && ldconfig

ENTRYPOINT ["/opt/entrypoint.sh"]
