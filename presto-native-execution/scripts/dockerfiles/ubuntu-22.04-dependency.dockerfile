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

ARG base=ubuntu:22.04
FROM ${base}

# Set a default timezone, can be overriden via ARG
ARG tz="America/New_York"
ARG DEBIAN_FRONTEND="noninteractive"
ENV PROMPT_ALWAYS_RESPOND=n
ENV SUDO=" "
# TZ and DEBIAN_FRONTEND="noninteractive"
# are required to avoid tzdata installation
# to prompt for region selection.
ENV TZ=${tz}

RUN mkdir -p /scripts /velox/scripts
COPY scripts /scripts
COPY velox/scripts /velox/scripts
# install rpm needed for minio install.
RUN mkdir build && \
    (cd build && ../scripts/setup-ubuntu.sh && \
                         apt install -y rpm && \
                 ../velox/scripts/setup-ubuntu.sh install_adapters && \
                 ../scripts/setup-adapters.sh ) && \
    rm -rf build
