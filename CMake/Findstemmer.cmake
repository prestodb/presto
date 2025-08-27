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

find_library(STEMMER_LIB libstemmer.a)
if("${STEMMER_LIB}" STREQUAL "STEMMER_LIB-NOTFOUND")
  set(stemmer_FOUND false)
  return()
endif()

set(stemmer_FOUND true)
if(NOT TARGET stemmer::stemmer)
  add_library(stemmer::stemmer STATIC IMPORTED GLOBAL)

  find_path(STEMMER_INCLUDE_PATH libstemmer.h)
  set_target_properties(
    stemmer::stemmer
    PROPERTIES
      IMPORTED_LOCATION ${STEMMER_LIB}
      INTERFACE_INCLUDE_DIRECTORIES ${STEMMER_INCLUDE_PATH}
  )
endif()
