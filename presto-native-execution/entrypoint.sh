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

# Extract pod index from POD_NAME or HOSTNAME
if [ -n "$POD_NAME" ]; then
    POD_INDEX="${POD_NAME##*-}"
elif [ -n "$HOSTNAME" ]; then
    POD_INDEX="${HOSTNAME##*-}"
else
    POD_INDEX=""
fi

# Set CUDA_VISIBLE_DEVICES if POD_INDEX is valid
if [ -n "$POD_INDEX" ] && [ "$POD_INDEX" -eq "$POD_INDEX" ] 2>/dev/null; then
    export CUDA_VISIBLE_DEVICES="$POD_INDEX"
    echo "Setting CUDA_VISIBLE_DEVICES=$CUDA_VISIBLE_DEVICES for pod index $POD_INDEX"
else
    echo "No valid pod index found, CUDA_VISIBLE_DEVICES not set"
fi

# Start Presto server
GLOG_logtostderr=1 presto_server --etc-dir=/opt/presto-server/etc
