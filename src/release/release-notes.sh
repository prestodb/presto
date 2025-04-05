#!/bin/bash
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
#


if [[ "$#" -ne 2 ]]; then
    echo "Usage: $0 <github_user> <github_access_token>"
    exit 1
fi

curl -L -o /tmp/presto_release "https://oss.sonatype.org/service/local/artifact/maven/redirect?g=com.facebook.presto&a=presto-release-tools&v=RELEASE&r=releases&c=executable&e=jar"
chmod 755 /tmp/presto_release
/tmp/presto_release release-notes --github-user $1 --github-access-token $2
