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

add_library(
  velox_dwio_dwrf_reader
  BinaryStreamReader.cpp
  ColumnReader.cpp
  DwrfData.cpp
  DwrfReader.cpp
  DwrfReaderShared.cpp
  FlatMapColumnReader.cpp
  FlatMapHelper.cpp
  ReaderBase.cpp
  SelectiveColumnReader.cpp
  SelectiveDwrfReader.cpp
  SelectiveByteRleColumnReader.cpp
  SelectiveIntegerDirectColumnReader.cpp
  SelectiveIntegerDictionaryColumnReader.cpp
  SelectiveStringDirectColumnReader.cpp
  SelectiveStringDictionaryColumnReader.cpp
  SelectiveTimestampColumnReader.cpp
  SelectiveStructColumnReader.cpp
  ColumnLoader.cpp
  SelectiveRepeatedColumnReader.cpp
  StripeDictionaryCache.cpp
  StripeReaderBase.cpp
  StripeStream.cpp)

target_link_libraries(
  velox_dwio_dwrf_reader velox_dwio_common velox_dwio_dwrf_common velox_caching
  velox_dwio_dwrf_utils ${FMT})
