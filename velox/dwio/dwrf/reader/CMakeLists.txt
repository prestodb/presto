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

velox_add_library(
  velox_dwio_dwrf_reader
  BinaryStreamReader.cpp
  ColumnReader.cpp
  DwrfData.cpp
  DwrfReader.cpp
  FlatMapColumnReader.cpp
  ReaderBase.cpp
  SelectiveDwrfReader.cpp
  SelectiveFlatMapColumnReader.cpp
  SelectiveIntegerDirectColumnReader.cpp
  SelectiveIntegerDictionaryColumnReader.cpp
  SelectiveStringDirectColumnReader.cpp
  SelectiveStringDictionaryColumnReader.cpp
  SelectiveTimestampColumnReader.cpp
  SelectiveDecimalColumnReader.cpp
  SelectiveStructColumnReader.cpp
  SelectiveRepeatedColumnReader.cpp
  StreamLabels.cpp
  StripeDictionaryCache.cpp
  StripeReaderBase.cpp
  StripeStream.cpp)

velox_link_libraries(
  velox_dwio_dwrf_reader
  velox_dwio_common
  velox_dwio_dwrf_common
  velox_caching
  velox_dwio_dwrf_utils
  velox_test_util
  fmt::fmt)
