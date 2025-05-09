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

FROM cp.stg.icr.io/cp/cpd/go-fips:ubi9-latest
ENV PROMPT_ALWAYS_RESPOND=n
ENV CC=/opt/rh/gcc-toolset-12/root/bin/gcc
ENV CXX=/opt/rh/gcc-toolset-12/root/bin/g++

ARG PRESTISSIMO_BUILD_THREADS
ENV BUILD_THREADS=${PRESTISSIMO_BUILD_THREADS}

ARG artifactory_user
ARG artifactory_token
ENV artifactory_user=${artifactory_user}
ENV artifactory_token=${artifactory_token}

RUN mkdir -p /scripts /velox/scripts
COPY scripts /scripts
COPY velox/scripts /velox/scripts
RUN ls /scripts
RUN mkdir build && \
    (cd build && /scripts/setup-redhat.sh && \
                 source /opt/rh/gcc-toolset-12/enable \
                 source /scripts/setup-magen.sh \
                 /velox/scripts/setup-adapters.sh aws gcs abfs && \
                 /scripts/setup-adapters.sh) && \
    rm -rf build
