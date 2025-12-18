#!/bin/sh
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

if [[ "$PROFILE" == "ON" ]]; then
  mkdir /presto_profiles

  if [[ -z $PROFILE_ARGS ]]; then
    PROFILE_ARGS="-t nvtx,cuda,osrt 
                  --cuda-memory-usage=true 
                  --cuda-um-cpu-page-faults=true 
                  --cuda-um-gpu-page-faults=true 
                  --cudabacktrace=true"
  fi
  PROFILE_CMD="nsys launch $PROFILE_ARGS"
fi

GLOG_logtostderr=1 $PROFILE_CMD presto_server --etc-dir=/opt/presto-server/etc
