# Copyright (c) Facebook, Inc. and its affiliates.
#
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
# Build the test and build container for presto_cpp
FROM quay.io/pypa/manylinux_2_28_x86_64:latest

COPY scripts/setup-helper-functions.sh /
COPY scripts/setup-manylinux.sh /

# Build static folly to reduce wheel size (folly.so is ~120M)
ENV  VELOX_BUILD_SHARED=OFF
# The removal of the build dir has to happen in the same layer as the build
# to minimize the image size. gh & jq are required for CI
RUN mkdir build && ( cd build && bash /setup-manylinux.sh ) && rm -rf build && \
        dnf install -y -q 'dnf-command(config-manager)' && \
        dnf config-manager --add-repo 'https://cli.github.com/packages/rpm/gh-cli.repo' && \
        dnf install -y -q gh jq && \
        dnf clean all

ENV LD_LIBRARY_PATH="/usr/local/lib:/usr/local/lib64:$LD_LIBRARY_PATH"
