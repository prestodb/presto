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

#include "velox/common/file/tests/FaultyFile.h"

namespace facebook::velox::tests::utils {

std::string FaultFileOperation::typeString(Type type) {
  switch (type) {
    case Type::kReadv:
      return "READV";
    case Type::kRead:
      return "READ";
    default:
      VELOX_UNSUPPORTED(
          "Unknown file operation type: {}", static_cast<int>(type));
      break;
  }
}

FaultyReadFile::FaultyReadFile(
    const std::string& path,
    std::shared_ptr<ReadFile> delegatedFile,
    FileFaultInjectionHook injectionHook,
    folly::Executor* executor)
    : path_(path),
      delegatedFile_(std::move(delegatedFile)),
      injectionHook_(std::move(injectionHook)),
      executor_(executor) {
  VELOX_CHECK_NOT_NULL(delegatedFile_);
}

std::string_view
FaultyReadFile::pread(uint64_t offset, uint64_t length, void* buf) const {
  if (injectionHook_ != nullptr) {
    FaultFileReadOperation op(path_, offset, length, buf);
    injectionHook_(&op);
    if (!op.delegate) {
      return std::string_view(static_cast<char*>(op.buf), op.length);
    }
  }
  return delegatedFile_->pread(offset, length, buf);
}

uint64_t FaultyReadFile::preadv(
    uint64_t offset,
    const std::vector<folly::Range<char*>>& buffers) const {
  if (injectionHook_ != nullptr) {
    FaultFileReadvOperation op(path_, offset, buffers);
    injectionHook_(&op);
    if (!op.delegate) {
      return op.readBytes;
    }
  }
  return delegatedFile_->preadv(offset, buffers);
}

folly::SemiFuture<uint64_t> FaultyReadFile::preadvAsync(
    uint64_t offset,
    const std::vector<folly::Range<char*>>& buffers) const {
  // TODO: add fault injection for async read later.
  if (delegatedFile_->hasPreadvAsync() || executor_ == nullptr) {
    return delegatedFile_->preadvAsync(offset, buffers);
  }
  auto promise = std::make_unique<folly::Promise<uint64_t>>();
  folly::SemiFuture<uint64_t> future = promise->getSemiFuture();
  executor_->add([this,
                  _promise = std::move(promise),
                  _offset = offset,
                  _buffers = buffers]() {
    auto delegateFuture = delegatedFile_->preadvAsync(_offset, _buffers);
    _promise->setValue(delegateFuture.wait().value());
  });
  return future;
}

FaultyWriteFile::FaultyWriteFile(
    const std::string& path,
    std::shared_ptr<WriteFile> delegatedFile,
    FileFaultInjectionHook injectionHook)
    : path_(path),
      delegatedFile_(std::move(delegatedFile)),
      injectionHook_(std::move(injectionHook)) {
  VELOX_CHECK_NOT_NULL(delegatedFile_);
}

void FaultyWriteFile::append(std::string_view data) {
  if (injectionHook_ != nullptr) {
    FaultFileWriteOperation op(path_, data);
    injectionHook_(&op);
    if (!op.delegate) {
      return;
    }
  }
  delegatedFile_->append(data);
}

void FaultyWriteFile::append(std::unique_ptr<folly::IOBuf> data) {
  delegatedFile_->append(std::move(data));
}

void FaultyWriteFile::flush() {
  delegatedFile_->flush();
}

void FaultyWriteFile::close() {
  delegatedFile_->close();
}
} // namespace facebook::velox::tests::utils
