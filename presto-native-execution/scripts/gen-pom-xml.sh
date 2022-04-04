#!/bin/bash
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

set -eufx -o pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd -P)
POM_FILE="${SCRIPT_DIR}/../java/presto-native-tests/pom.xml"
POM_FILE_TEMPLATE="${SCRIPT_DIR}/../java/presto-native-tests/pom.xml.template"

PRESTO_VERSION=
POM_XML=$(curl -s -f https://raw.githubusercontent.com/prestodb/presto/master/pom.xml 2> /dev/null) || true

if [[ -n ${POM_XML} ]]
then
    PRESTO_VERSION=$(echo "${POM_XML}" | sed -e 's/xmlns="[^"]*"//g' | xmllint --xpath '/project/version/text()' -)
fi

if [[ -n ${PRESTO_VERSION} ]]
then
    sed "s/<version>0\.[0-9]*-SNAPSHOT<\/version>/<version>${PRESTO_VERSION}<\/version>/g" < "${POM_FILE_TEMPLATE}" > "${POM_FILE}"
else
    # couldn't get the latest presto version from github
    # just copy the template file over
    cp "${POM_FILE_TEMPLATE}" "${POM_FILE}"
    # attempt to update the version using maven
    # this may require proper local maven setup
    mvn versions:update-parent -DallowSnapshots=true -Djava.net.preferIPv6Addresses=true -f "${POM_FILE}"
fi
