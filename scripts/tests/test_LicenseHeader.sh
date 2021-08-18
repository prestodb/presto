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

SRC=$(dirname $0)
DATA=$SRC/data

source $SRC/TestFramework.sh

LICENSE_HEADER=$SRC/../license-header.py
LICENSE_HEADER_FILE=$SRC/../../license.header

license_header() {
    $SRC/../license-header.py --header $LICENSE_HEADER_FILE "$@"
}

TestFile() {
    local title="$1"        ; shift
    local file="$1"         ; shift
    local expected="$1"     ; shift

    Test "license-header.py $title file $file"
        cp $DATA/$file .
        license_header "$@" -i $file

    DiffFiles $file $DATA/$expected
}

# Test header insertion
#
TestFile Insert foo.cpp              foo.expected.cpp
TestFile Insert foo.h                foo.expected.h
TestFile Insert foo.sh               foo.expected.sh
TestFile Insert foo.py               foo.expected.py
TestFile Insert hashbang.sh          hashbang.expected.sh
TestFile Insert hashbang.py          hashbang.expected.py
TestFile Insert CMakeLists.txt       CMakeLists.expected.txt

# Test header found - don't change file further
#
TestFile Again foo.expected.cpp     foo.expected.cpp
TestFile Again foo.expected.h       foo.expected.h
TestFile Again foo.expected.sh      foo.expected.sh
TestFile Again foo.expected.py      foo.expected.py
TestFile Again hashbang.expected.sh hashbang.expected.sh
TestFile Again hashbang.expected.py hashbang.expected.py

TestFile Remove foo.expected.cpp     foo.cpp      --remove
TestFile Remove foo.expected.h       foo.h        --remove
TestFile Remove foo.expected.sh      foo.sh       --remove
TestFile Remove foo.expected.py      foo.py       --remove
TestFile Remove hashbang.expected.sh hashbang.sh  --remove
TestFile Remove hashbang.expected.py hashbang.py  --remove

# Test header found - Close match
#
TestFile Almost foo.almost.cpp     foo.expected.cpp
TestFile Almost foo.almost.sh      foo.expected.sh
TestFile Almost hashbang.almost.sh hashbang.expected.sh

Test "List of Files in stdin"
    cp $DATA/foo.sh .

    echo foo.sh | license_header -i -

    DiffFiles foo.sh $DATA/foo.expected.sh

Test "Header Check Only - OK"
    cp $DATA/foo.expected.sh .
    cp $DATA/foo.expected.cpp .

    if license_header "$@" -k foo.expected.sh foo.expected.cpp ; then
        Pass
    else
        Fail
    fi

Test "Header Check Only - Fix"
    cp $DATA/foo.sh .
    cp $DATA/foo.cpp .

    if license_header "$@" -k foo.sh foo.cpp; then
        Fail
    else
        Pass
    fi

Test "Header Check Verbose"
    cp $DATA/foo.sh .
    cp $DATA/foo.expected.sh .
    cp $DATA/foo.cpp .
    cp $DATA/foo.expected.cpp .

    result=$(license_header "$@" -vk foo.sh foo.expected.sh foo.cpp foo.expected.cpp)
    expected="\
Fix  : foo.sh
OK   : foo.expected.sh
Fix  : foo.cpp
OK   : foo.expected.cpp"

    CompareArgs "$result" "$expected"

TestDone
