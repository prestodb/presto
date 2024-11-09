/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "velox/exec/tests/utils/SerializedPageUtil.h"

using namespace facebook::velox;

namespace facebook::velox::exec::test {

std::unique_ptr<SerializedPage> toSerializedPage(
    const RowVectorPtr& vector,
    VectorSerde::Kind serdeKind,
    const std::shared_ptr<OutputBufferManager>& bufferManager,
    memory::MemoryPool* pool) {
  auto data =
      std::make_unique<VectorStreamGroup>(pool, getNamedVectorSerde(serdeKind));
  auto size = vector->size();
  auto range = IndexRange{0, size};
  data->createStreamTree(asRowType(vector->type()), size);
  data->append(vector, folly::Range(&range, 1));
  auto listener = bufferManager->newListener();
  IOBufOutputStream stream(*pool, listener.get(), data->size());
  data->flush(&stream);
  return std::make_unique<SerializedPage>(stream.getIOBuf(), nullptr, size);
}

} // namespace facebook::velox::exec::test
