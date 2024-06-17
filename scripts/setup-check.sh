#!/bin/bash
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

set -e
set -x

export DEBIAN_FRONTEND=noninteractive
apt update
apt install --no-install-recommends -y clang-format-18 python3-pip git make ssh
pip3 install --break-system-packages cmake==3.28.3 cmake_format black regex
pip3 cache purge
apt purge --auto-remove -y python3-pip
update-alternatives --install /usr/bin/clang-format clang-format "$(command -v clang-format-18)" 18
apt clean
