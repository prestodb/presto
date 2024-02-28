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

# for generated headers

add_library(velox_gcs RegisterGCSFileSystem.cpp)

if(VELOX_ENABLE_GCS)
  target_sources(velox_gcs PRIVATE GCSFileSystem.cpp GCSUtil.cpp)
  target_link_libraries(velox_gcs velox_exception Folly::folly
                        google-cloud-cpp::storage)

  if(${VELOX_BUILD_TESTING})
    add_subdirectory(tests)
  endif()

  if(${VELOX_ENABLE_EXAMPLES})
    add_subdirectory(examples)
  endif()
endif()
