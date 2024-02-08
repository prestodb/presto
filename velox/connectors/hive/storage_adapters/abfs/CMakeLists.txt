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

add_library(velox_abfs RegisterAbfsFileSystem.cpp)

if(VELOX_ENABLE_ABFS)
  target_sources(velox_abfs PRIVATE AbfsFileSystem.cpp AbfsWriteFile.cpp
                                    AbfsUtils.cpp)
  target_link_libraries(
    velox_abfs
    PUBLIC velox_file
           velox_core
           velox_hive_config
           velox_dwio_common_exception
           Azure::azure-storage-blobs
           Azure::azure-storage-files-datalake
           Folly::folly
           glog::glog
           fmt::fmt)

  if(${VELOX_BUILD_TESTING})
    add_subdirectory(tests)
  endif()
endif()
