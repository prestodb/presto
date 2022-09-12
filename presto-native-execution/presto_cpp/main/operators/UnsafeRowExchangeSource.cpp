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
#include "presto_cpp/main/operators/UnsafeRowExchangeSource.h"

namespace facebook::presto::operators {

void UnsafeRowExchangeSource::request() {
  std::lock_guard<std::mutex> l(queue_->mutex());

  if (!shuffle_->hasNext(destination_)) {
    atEnd_ = true;
    queue_->enqueue(nullptr);
    return;
  }

  auto buffer = shuffle_->next(destination_, true);

  auto ioBuf = folly::IOBuf::wrapBuffer(buffer->as<char>(), buffer->size());
  queue_->enqueue(std::make_unique<velox::exec::SerializedPage>(
      std::move(ioBuf), pool_, [buffer](auto&) { buffer->release(); }));
}
}; // namespace facebook::presto::operators