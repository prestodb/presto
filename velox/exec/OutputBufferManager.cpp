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
#include "velox/exec/OutputBufferManager.h"
#include "velox/exec/Task.h"

namespace facebook::velox::exec {

// static
std::weak_ptr<OutputBufferManager> OutputBufferManager::getInstance() {
  static auto kInstance = std::make_shared<OutputBufferManager>();
  return kInstance;
}

std::shared_ptr<OutputBuffer> OutputBufferManager::getBuffer(
    const std::string& taskId) {
  return buffers_.withLock([&](auto& buffers) {
    auto it = buffers.find(taskId);
    VELOX_CHECK(
        it != buffers.end(), "Output buffers for task not found: {}", taskId);
    return it->second;
  });
}

std::shared_ptr<OutputBuffer> OutputBufferManager::getBufferIfExists(
    const std::string& taskId) {
  return buffers_.withLock([&](auto& buffers) {
    auto it = buffers.find(taskId);
    return it == buffers.end() ? nullptr : it->second;
  });
}

uint64_t OutputBufferManager::numBuffers() const {
  return buffers_.lock()->size();
}

bool OutputBufferManager::enqueue(
    const std::string& taskId,
    int destination,
    std::unique_ptr<SerializedPage> data,
    ContinueFuture* future) {
  return getBuffer(taskId)->enqueue(destination, std::move(data), future);
}

void OutputBufferManager::noMoreData(const std::string& taskId) {
  getBuffer(taskId)->noMoreData();
}

bool OutputBufferManager::isFinished(const std::string& taskId) {
  return getBuffer(taskId)->isFinished();
}

void OutputBufferManager::acknowledge(
    const std::string& taskId,
    int destination,
    int64_t sequence) {
  auto buffer =
      buffers_.withLock([&](auto& buffers) -> std::shared_ptr<OutputBuffer> {
        auto it = buffers.find(taskId);
        if (it == buffers.end()) {
          VLOG(1) << "Receiving ack for non-existent task " << taskId
                  << " destination " << destination << " sequence " << sequence;
          return nullptr;
        }
        return it->second;
      });
  if (buffer) {
    buffer->acknowledge(destination, sequence);
  }
}

void OutputBufferManager::deleteResults(
    const std::string& taskId,
    int destination) {
  if (auto buffer = getBufferIfExists(taskId)) {
    buffer->deleteResults(destination);
  }
}

bool OutputBufferManager::getData(
    const std::string& taskId,
    int destination,
    uint64_t maxBytes,
    int64_t sequence,
    DataAvailableCallback notify,
    DataConsumerActiveCheckCallback activeCheck) {
  if (auto buffer = getBufferIfExists(taskId)) {
    buffer->getData(destination, maxBytes, sequence, notify, activeCheck);
    return true;
  }
  return false;
}

void OutputBufferManager::initializeTask(
    std::shared_ptr<Task> task,
    core::PartitionedOutputNode::Kind kind,
    int numDestinations,
    int numDrivers) {
  const auto& taskId = task->taskId();

  buffers_.withLock([&](auto& buffers) {
    auto it = buffers.find(taskId);
    if (it == buffers.end()) {
      buffers[taskId] = std::make_shared<OutputBuffer>(
          std::move(task), kind, numDestinations, numDrivers);
    } else {
      VELOX_FAIL(
          "Registering an output buffer for pre-existing taskId {}", taskId);
    }
  });
}

bool OutputBufferManager::updateOutputBuffers(
    const std::string& taskId,
    int numBuffers,
    bool noMoreBuffers) {
  if (auto buffer = getBufferIfExists(taskId)) {
    buffer->updateOutputBuffers(numBuffers, noMoreBuffers);
    return true;
  }
  return false;
}

bool OutputBufferManager::updateNumDrivers(
    const std::string& taskId,
    uint32_t newNumDrivers) {
  if (auto buffer = getBufferIfExists(taskId)) {
    buffer->updateNumDrivers(newNumDrivers);
    return true;
  }
  return false;
}

void OutputBufferManager::removeTask(const std::string& taskId) {
  auto buffer =
      buffers_.withLock([&](auto& buffers) -> std::shared_ptr<OutputBuffer> {
        auto it = buffers.find(taskId);
        if (it == buffers.end()) {
          // Already removed.
          return nullptr;
        }
        auto taskBuffer = it->second;
        buffers.erase(taskId);
        return taskBuffer;
      });
  if (buffer != nullptr) {
    buffer->terminate();
  }
}

std::string OutputBufferManager::toString() {
  return buffers_.withLock([](const auto& buffers) {
    std::stringstream out;
    out << "[BufferManager:" << std::endl;
    for (const auto& pair : buffers) {
      out << pair.first << ": " << pair.second->toString() << std::endl;
    }
    out << "]";
    return out.str();
  });
}

double OutputBufferManager::getUtilization(const std::string& taskId) {
  auto buffer = getBufferIfExists(taskId);
  if (buffer != nullptr) {
    return buffer->getUtilization();
  }
  return 0;
}

bool OutputBufferManager::isOverutilized(const std::string& taskId) {
  auto buffer = getBufferIfExists(taskId);
  if (buffer != nullptr) {
    return buffer->isOverutilized();
  }
  return false;
}

std::optional<OutputBuffer::Stats> OutputBufferManager::stats(
    const std::string& taskId) {
  auto buffer = getBufferIfExists(taskId);
  if (buffer != nullptr) {
    return buffer->stats();
  }
  return std::nullopt;
}

} // namespace facebook::velox::exec
