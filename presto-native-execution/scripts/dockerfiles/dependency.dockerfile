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

ARG BASE_IMAGE=quay.io/centos/centos:stream8
FROM ${BASE_IMAGE}

ARG BUILD_TYPE=Release
ARG OSNAME=centos

ENV PROMPT_ALWAYS_RESPOND=n

RUN mkdir -p /scripts /velox/scripts
COPY scripts /scripts
COPY velox/scripts /velox/scripts
RUN mkdir build && (cd build && \
    if [ "${OSNAME}" = "centos" ] ; then \
        ../velox/scripts/setup-centos8.sh ; \
        export CC=/opt/rh/gcc-toolset-9/root/bin/gcc ; \
        export CXX=/opt/rh/gcc-toolset-9/root/bin/g++ ; \
    fi && ../scripts/setup-${OSNAME}.sh && ../velox/scripts/setup-adapters.sh) && rm -rf build
