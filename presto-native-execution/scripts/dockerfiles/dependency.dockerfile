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

ARG BUILD_TYPE=Release

FROM quay.io/centos/centos:stream8

ENV PROMPT_ALWAYS_RESPOND=n
ENV BUILD_TYPE=${BUILD_TYPE}
ENV CC=/opt/rh/gcc-toolset-9/root/bin/gcc
ENV CXX=/opt/rh/gcc-toolset-9/root/bin/g++

COPY setup-helper-functions.sh /
COPY velox-setup-centos.sh /
RUN chmod +x ./velox-setup-centos.sh
RUN mkdir build && ( cd build && ../velox-setup-centos.sh ) && rm -rf build
COPY setup-centos.sh /
RUN chmod +x ./setup-centos.sh
RUN mkdir build && ( cd build && ../setup-centos.sh ) && rm -rf build
COPY setup-adapters.sh /
RUN chmod +x ./setup-adapters.sh
RUN mkdir build && ( cd build && ../setup-adapters.sh ) && rm -rf build
