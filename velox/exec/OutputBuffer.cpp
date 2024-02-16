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
#include "velox/exec/OutputBuffer.h"
#include "velox/core/QueryConfig.h"
#include "velox/exec/Task.h"

namespace facebook::velox::exec {

using core::PartitionedOutputNode;

void ArbitraryBuffer::noMoreData() {
  // Drop duplicate end markers.
  if (!pages_.empty() && pages_.back() == nullptr) {
    return;
  }
  pages_.push_back(nullptr);
}

void ArbitraryBuffer::enqueue(std::unique_ptr<SerializedPage> page) {
  VELOX_CHECK_NOT_NULL(page, "Unexpected null page");
  VELOX_CHECK(!hasNoMoreData(), "Arbitrary buffer has set no more data marker");
  pages_.push_back(std::shared_ptr<SerializedPage>(page.release()));
}

void ArbitraryBuffer::getAvailablePageSizes(std::vector<int64_t>& out) const {
  out.reserve(out.size() + pages_.size());
  for (const auto& page : pages_) {
    if (page != nullptr) {
      out.push_back(page->size());
    }
  }
}

std::vector<std::shared_ptr<SerializedPage>> ArbitraryBuffer::getPages(
    uint64_t maxBytes) {
  VELOX_CHECK_GT(maxBytes, 0, "maxBytes can't be zero");

  std::vector<std::shared_ptr<SerializedPage>> pages;
  uint64_t bytesRemoved{0};
  while (bytesRemoved < maxBytes && !pages_.empty()) {
    if (pages_.front() == nullptr) {
      // NOTE: keep the end marker in arbitrary buffer to signal all the
      // destination buffers after the buffers have all been consumed.
      VELOX_CHECK_EQ(pages_.size(), 1);
      pages.push_back(nullptr);
      break;
    }
    bytesRemoved += pages_.front()->size();
    pages.push_back(std::move(pages_.front()));
    pages_.pop_front();
  }
  return pages;
}

std::string ArbitraryBuffer::toString() const {
  return fmt::format(
      "[ARBITRARY_BUFFER PAGES[{}] NO MORE DATA[{}]]",
      pages_.size() - !!hasNoMoreData(),
      hasNoMoreData());
}

void DestinationBuffer::Stats::recordEnqueue(const SerializedPage& data) {
  const auto numRows = data.numRows();
  VELOX_CHECK(numRows.has_value(), "SerializedPage's numRows must be valid");
  bytesBuffered += data.size();
  rowsBuffered += numRows.value();
  ++pagesBuffered;
}

void DestinationBuffer::Stats::recordAcknowledge(const SerializedPage& data) {
  const auto numRows = data.numRows();
  VELOX_CHECK(numRows.has_value(), "SerializedPage's numRows must be valid");
  const int64_t size = data.size();
  bytesBuffered -= size;
  VELOX_DCHECK_GE(bytesBuffered, 0, "bytesBuffered must be non-negative");
  rowsBuffered -= numRows.value();
  VELOX_DCHECK_GE(rowsBuffered, 0, "rowsBuffered must be non-negative");
  --pagesBuffered;
  VELOX_DCHECK_GE(pagesBuffered, 0, "pagesBuffered must be non-negative");
  bytesSent += size;
  rowsSent += numRows.value();
  ++pagesSent;
}

void DestinationBuffer::Stats::recordDelete(const SerializedPage& data) {
  recordAcknowledge(data);
}

DestinationBuffer::Data DestinationBuffer::getData(
    uint64_t maxBytes,
    int64_t sequence,
    DataAvailableCallback notify,
    DataConsumerActiveCheckCallback activeCheck,
    ArbitraryBuffer* arbitraryBuffer) {
  VELOX_CHECK_GE(
      sequence, sequence_, "Get received for an already acknowledged item");
  VELOX_CHECK_GT(maxBytes, 0);
  if (arbitraryBuffer != nullptr) {
    loadData(arbitraryBuffer, maxBytes);
  }

  if (sequence - sequence_ > data_.size()) {
    VLOG(1) << this << " Out of order get: " << sequence << " over "
            << sequence_ << " Setting second notify " << notifySequence_
            << " / " << sequence;
    notify_ = std::move(notify);
    aliveCheck_ = std::move(activeCheck);
    notifySequence_ = std::min(notifySequence_, sequence);
    notifyMaxBytes_ = maxBytes;
    return {};
  }

  if (sequence - sequence_ == data_.size()) {
    notify_ = std::move(notify);
    aliveCheck_ = std::move(activeCheck);
    notifySequence_ = sequence;
    notifyMaxBytes_ = maxBytes;
    return {};
  }

  std::vector<std::unique_ptr<folly::IOBuf>> data;
  uint64_t resultBytes = 0;
  auto i = sequence - sequence_;
  for (; i < data_.size(); ++i) {
    // nullptr is used as end marker
    if (data_[i] == nullptr) {
      VELOX_CHECK_EQ(i, data_.size() - 1, "null marker found in the middle");
      data.push_back(nullptr);
      ++i;
      break;
    }
    data.push_back(data_[i]->getIOBuf());
    resultBytes += data_[i]->size();
    if (resultBytes >= maxBytes) {
      ++i;
      break;
    }
  }
  bool atEnd = false;
  std::vector<int64_t> remainingBytes;
  remainingBytes.reserve(data_.size() - i);
  for (; i < data_.size(); ++i) {
    if (data_[i] == nullptr) {
      VELOX_CHECK_EQ(i, data_.size() - 1, "null marker found in the middle");
      atEnd = true;
      break;
    }
    remainingBytes.push_back(data_[i]->size());
  }
  if (!atEnd && arbitraryBuffer) {
    arbitraryBuffer->getAvailablePageSizes(remainingBytes);
  }
  return {std::move(data), std::move(remainingBytes), true};
}

void DestinationBuffer::enqueue(std::shared_ptr<SerializedPage> data) {
  // Drop duplicate end markers.
  if (data == nullptr && !data_.empty() && data_.back() == nullptr) {
    return;
  }

  if (data != nullptr) {
    stats_.recordEnqueue(*data);
  }
  data_.push_back(std::move(data));
}

DataAvailable DestinationBuffer::getAndClearNotify() {
  if (notify_ == nullptr) {
    VELOX_CHECK_NULL(aliveCheck_);
    return DataAvailable();
  }
  DataAvailable result;
  result.callback = notify_;
  result.sequence = notifySequence_;
  auto data = getData(notifyMaxBytes_, notifySequence_, nullptr, nullptr);
  result.data = std::move(data.data);
  result.remainingBytes = std::move(data.remainingBytes);
  clearNotify();
  return result;
}

void DestinationBuffer::clearNotify() {
  notify_ = nullptr;
  aliveCheck_ = nullptr;
  notifySequence_ = 0;
  notifyMaxBytes_ = 0;
}

void DestinationBuffer::finish() {
  VELOX_CHECK_NULL(notify_, "notify must be cleared before finish");
  VELOX_CHECK(data_.empty(), "data must be fetched before finish");
  stats_.finished = true;
}

void DestinationBuffer::maybeLoadData(ArbitraryBuffer* buffer) {
  VELOX_CHECK(!buffer->empty() || buffer->hasNoMoreData());
  if (notify_ == nullptr) {
    return;
  }
  if (aliveCheck_ != nullptr && !aliveCheck_()) {
    // Skip load data to an inactive destination buffer.
    clearNotify();
    return;
  }
  VELOX_CHECK_GT(notifyMaxBytes_, 0);
  loadData(buffer, notifyMaxBytes_);
}

void DestinationBuffer::loadData(ArbitraryBuffer* buffer, uint64_t maxBytes) {
  auto pages = buffer->getPages(maxBytes);
  for (auto& page : pages) {
    enqueue(std::move(page));
  }
}

std::vector<std::shared_ptr<SerializedPage>> DestinationBuffer::acknowledge(
    int64_t sequence,
    bool fromGetData) {
  const int64_t numDeleted = sequence - sequence_;
  if (numDeleted == 0 && fromGetData) {
    // If called from getData, it is expected that there will be
    // nothing to delete because a previous acknowledgement has been
    // received before the getData. This is not guaranteed though
    // because the messages may arrive out of order. Note that getData
    // implicitly acknowledges all messages with a lower sequence
    // number than the one in getData.
    return {};
  }
  if (numDeleted <= 0) {
    // Acknowledges come out of order, e.g. ack of 10 and 9 have
    // swapped places in flight.
    VLOG(1) << this << " Out of order ack: " << sequence << " over "
            << sequence_;
    return {};
  }

  VELOX_CHECK_LE(
      numDeleted, data_.size(), "Ack received for a not yet produced item");
  std::vector<std::shared_ptr<SerializedPage>> freed;
  for (auto i = 0; i < numDeleted; ++i) {
    if (data_[i] == nullptr) {
      VELOX_CHECK_EQ(i, data_.size() - 1, "null marker found in the middle");
      break;
    }
    stats_.recordAcknowledge(*data_[i]);
    freed.push_back(std::move(data_[i]));
  }
  data_.erase(data_.begin(), data_.begin() + numDeleted);
  sequence_ += numDeleted;
  return freed;
}

std::vector<std::shared_ptr<SerializedPage>>
DestinationBuffer::deleteResults() {
  std::vector<std::shared_ptr<SerializedPage>> freed;
  for (auto i = 0; i < data_.size(); ++i) {
    if (data_[i] == nullptr) {
      VELOX_CHECK_EQ(i, data_.size() - 1, "null marker found in the middle");
      break;
    }
    stats_.recordDelete(*data_[i]);
    freed.push_back(std::move(data_[i]));
  }
  data_.clear();
  return freed;
}

DestinationBuffer::Stats DestinationBuffer::stats() const {
  return stats_;
}

std::string DestinationBuffer::toString() {
  std::stringstream out;
  out << "[available: " << data_.size() << ", "
      << "sequence: " << sequence_ << ", "
      << (notify_ ? "notify registered, " : "") << this << "]";
  return out.str();
}

namespace {
// Frees 'freed' and realizes 'promises'. Used after
// updateAfterAcknowledgeLocked. This runs outside of the mutex, so
// that we do the expensive free outside and only then continue the
// producers which will allocate more memory.
void releaseAfterAcknowledge(
    std::vector<std::shared_ptr<SerializedPage>>& freed,
    std::vector<ContinuePromise>& promises) {
  freed.clear();
  for (auto& promise : promises) {
    promise.setValue();
  }
}

} // namespace

OutputBuffer::OutputBuffer(
    std::shared_ptr<Task> task,
    PartitionedOutputNode::Kind kind,
    int numDestinations,
    uint32_t numDrivers)
    : task_(std::move(task)),
      kind_(kind),
      maxSize_(task_->queryCtx()->queryConfig().maxOutputBufferSize()),
      continueSize_((maxSize_ * kContinuePct) / 100),
      arbitraryBuffer_(
          isArbitrary() ? std::make_unique<ArbitraryBuffer>() : nullptr),
      numDrivers_(numDrivers) {
  buffers_.reserve(numDestinations);
  for (int i = 0; i < numDestinations; i++) {
    buffers_.push_back(std::make_unique<DestinationBuffer>());
  }
  finishedBufferStats_.resize(numDestinations);
}

void OutputBuffer::updateOutputBuffers(int numBuffers, bool noMoreBuffers) {
  if (isPartitioned()) {
    VELOX_CHECK_EQ(buffers_.size(), numBuffers);
    VELOX_CHECK(noMoreBuffers);
    noMoreBuffers_ = true;
    return;
  }

  std::vector<ContinuePromise> promises;
  bool isFinished;
  {
    std::lock_guard<std::mutex> l(mutex_);

    if (numBuffers > buffers_.size()) {
      addOutputBuffersLocked(numBuffers);
    }

    if (!noMoreBuffers) {
      return;
    }

    noMoreBuffers_ = true;
    isFinished = isFinishedLocked();
    updateAfterAcknowledgeLocked(dataToBroadcast_, promises);
  }

  releaseAfterAcknowledge(dataToBroadcast_, promises);
  if (isFinished) {
    task_->setAllOutputConsumed();
  }
}

void OutputBuffer::updateNumDrivers(uint32_t newNumDrivers) {
  bool isNoMoreDrivers{false};
  {
    std::lock_guard<std::mutex> l(mutex_);
    numDrivers_ = newNumDrivers;
    // If we finished all drivers, ensure we register that we are 'done'.
    if (numDrivers_ == numFinished_) {
      isNoMoreDrivers = true;
    }
  }
  if (isNoMoreDrivers) {
    noMoreDrivers();
  }
}

void OutputBuffer::addOutputBuffersLocked(int numBuffers) {
  VELOX_CHECK(!noMoreBuffers_);
  VELOX_CHECK(!isPartitioned());
  buffers_.reserve(numBuffers);
  for (int32_t i = buffers_.size(); i < numBuffers; ++i) {
    auto buffer = std::make_unique<DestinationBuffer>();
    if (isBroadcast()) {
      for (const auto& data : dataToBroadcast_) {
        buffer->enqueue(data);
      }
      if (atEnd_) {
        buffer->enqueue(nullptr);
      }
    }
    buffers_.emplace_back(std::move(buffer));
  }
  finishedBufferStats_.resize(numBuffers);
}

void OutputBuffer::updateStatsWithEnqueuedPageLocked(
    int64_t pageBytes,
    int64_t pageRows) {
  updateTotalBufferedBytesMsLocked();

  bufferedBytes_ += pageBytes;
  ++bufferedPages_;

  ++numOutputPages_;
  numOutputRows_ += pageRows;
  numOutputBytes_ += pageBytes;
}

void OutputBuffer::updateStatsWithFreedPagesLocked(
    int numPages,
    int64_t pageBytes) {
  updateTotalBufferedBytesMsLocked();

  bufferedBytes_ -= pageBytes;
  VELOX_CHECK_GE(bufferedBytes_, 0);
  bufferedPages_ -= numPages;
  VELOX_CHECK_GE(bufferedPages_, 0);
}

void OutputBuffer::updateTotalBufferedBytesMsLocked() {
  const auto nowMs = getCurrentTimeMs();
  if (bufferedBytes_ > 0) {
    const auto deltaMs = nowMs - bufferStartMs_;
    totalBufferedBytesMs_ += bufferedBytes_ * deltaMs;
  }

  bufferStartMs_ = nowMs;
}

bool OutputBuffer::enqueue(
    int destination,
    std::unique_ptr<SerializedPage> data,
    ContinueFuture* future) {
  VELOX_CHECK_NOT_NULL(data);
  VELOX_CHECK(
      task_->isRunning(), "Task is terminated, cannot add data to output.");
  std::vector<DataAvailable> dataAvailableCallbacks;
  bool blocked = false;
  {
    std::lock_guard<std::mutex> l(mutex_);
    VELOX_CHECK_LT(destination, buffers_.size());

    updateStatsWithEnqueuedPageLocked(data->size(), data->numRows().value());

    switch (kind_) {
      case PartitionedOutputNode::Kind::kBroadcast:
        VELOX_CHECK_EQ(destination, 0, "Bad destination {}", destination);
        enqueueBroadcastOutputLocked(std::move(data), dataAvailableCallbacks);
        break;
      case PartitionedOutputNode::Kind::kArbitrary:
        VELOX_CHECK_EQ(destination, 0, "Bad destination {}", destination);
        enqueueArbitraryOutputLocked(std::move(data), dataAvailableCallbacks);
        break;
      case PartitionedOutputNode::Kind::kPartitioned:
        enqueuePartitionedOutputLocked(
            destination, std::move(data), dataAvailableCallbacks);
        break;
      default:
        VELOX_UNREACHABLE(PartitionedOutputNode::kindString(kind_));
    }

    if (bufferedBytes_ > maxSize_ && future) {
      promises_.emplace_back("OutputBuffer::enqueue");
      *future = promises_.back().getSemiFuture();
      blocked = true;
    }
  }

  // Outside mutex_.
  for (auto& callback : dataAvailableCallbacks) {
    callback.notify();
  }

  return blocked;
}

void OutputBuffer::enqueueBroadcastOutputLocked(
    std::unique_ptr<SerializedPage> data,
    std::vector<DataAvailable>& dataAvailableCbs) {
  VELOX_DCHECK(isBroadcast());
  VELOX_CHECK_NULL(arbitraryBuffer_);
  VELOX_DCHECK(dataAvailableCbs.empty());

  std::shared_ptr<SerializedPage> sharedData(data.release());
  for (auto& buffer : buffers_) {
    if (buffer != nullptr) {
      buffer->enqueue(sharedData);
      dataAvailableCbs.emplace_back(buffer->getAndClearNotify());
    }
  }

  // NOTE: we don't need to add new buffer to 'dataToBroadcast_' if there is no
  // more output buffers.
  if (!noMoreBuffers_) {
    dataToBroadcast_.emplace_back(sharedData);
  }
}

void OutputBuffer::enqueueArbitraryOutputLocked(
    std::unique_ptr<SerializedPage> data,
    std::vector<DataAvailable>& dataAvailableCbs) {
  VELOX_DCHECK(isArbitrary());
  VELOX_DCHECK_NOT_NULL(arbitraryBuffer_);
  VELOX_DCHECK(dataAvailableCbs.empty());
  VELOX_CHECK(!arbitraryBuffer_->hasNoMoreData());

  arbitraryBuffer_->enqueue(std::move(data));
  VELOX_CHECK_LT(nextArbitraryLoadBufferIndex_, buffers_.size());
  int32_t bufferId = nextArbitraryLoadBufferIndex_;
  for (int32_t i = 0; i < buffers_.size();
       ++i, bufferId = (bufferId + 1) % buffers_.size()) {
    if (arbitraryBuffer_->empty()) {
      nextArbitraryLoadBufferIndex_ = bufferId;
      break;
    }
    auto* buffer = buffers_[bufferId].get();
    if (buffer == nullptr) {
      continue;
    }
    buffer->maybeLoadData(arbitraryBuffer_.get());
    dataAvailableCbs.emplace_back(buffer->getAndClearNotify());
  }
}

void OutputBuffer::enqueuePartitionedOutputLocked(
    int destination,
    std::unique_ptr<SerializedPage> data,
    std::vector<DataAvailable>& dataAvailableCbs) {
  VELOX_DCHECK(isPartitioned());
  VELOX_CHECK_NULL(arbitraryBuffer_);
  VELOX_DCHECK(dataAvailableCbs.empty());

  VELOX_CHECK_LT(destination, buffers_.size());
  auto* buffer = buffers_[destination].get();
  if (buffer != nullptr) {
    buffer->enqueue(std::move(data));
    dataAvailableCbs.emplace_back(buffer->getAndClearNotify());
  } else {
    // Some downstream tasks may finish early and delete the corresponding
    // buffers. Further data for these buffers is dropped.
    updateStatsWithFreedPagesLocked(1, data->size());
  }
}

void OutputBuffer::noMoreData() {
  // Increment number of finished drivers.
  checkIfDone(true);
}

void OutputBuffer::noMoreDrivers() {
  // Do not increment number of finished drivers.
  checkIfDone(false);
}

void OutputBuffer::checkIfDone(bool oneDriverFinished) {
  std::vector<DataAvailable> finished;
  {
    std::lock_guard<std::mutex> l(mutex_);
    if (oneDriverFinished) {
      ++numFinished_;
    }
    VELOX_CHECK_LE(
        numFinished_,
        numDrivers_,
        "Each driver should call noMoreData exactly once");
    atEnd_ = numFinished_ == numDrivers_;
    if (!atEnd_) {
      return;
    }
    if (isArbitrary()) {
      arbitraryBuffer_->noMoreData();
      for (auto& buffer : buffers_) {
        if (buffer != nullptr) {
          buffer->maybeLoadData(arbitraryBuffer_.get());
          finished.push_back(buffer->getAndClearNotify());
        }
      }
    } else {
      for (auto& buffer : buffers_) {
        if (buffer != nullptr) {
          buffer->enqueue(nullptr);
          finished.push_back(buffer->getAndClearNotify());
        }
      }
    }
  }

  // Notify outside of mutex.
  for (auto& notification : finished) {
    notification.notify();
  }
}

bool OutputBuffer::isFinished() {
  std::lock_guard<std::mutex> l(mutex_);
  return isFinishedLocked();
}

bool OutputBuffer::isFinishedLocked() {
  // NOTE: for broadcast output buffer, we can only mark it as finished after
  // receiving the no more (destination) buffers signal.
  if (isBroadcast() && !noMoreBuffers_) {
    return false;
  }
  for (auto& buffer : buffers_) {
    if (buffer != nullptr) {
      return false;
    }
  }
  return true;
}

void OutputBuffer::acknowledge(int destination, int64_t sequence) {
  std::vector<std::shared_ptr<SerializedPage>> freed;
  std::vector<ContinuePromise> promises;
  {
    std::lock_guard<std::mutex> l(mutex_);
    VELOX_CHECK_LT(destination, buffers_.size());
    auto* buffer = buffers_[destination].get();
    if (!buffer) {
      VLOG(1) << "Ack received after final ack for destination " << destination
              << " and sequence " << sequence;
      return;
    }
    freed = buffer->acknowledge(sequence, false);
    updateAfterAcknowledgeLocked(freed, promises);
  }
  releaseAfterAcknowledge(freed, promises);
}

void OutputBuffer::updateAfterAcknowledgeLocked(
    const std::vector<std::shared_ptr<SerializedPage>>& freed,
    std::vector<ContinuePromise>& promises) {
  uint64_t freedBytes{0};
  int freedPages{0};
  for (const auto& free : freed) {
    if (free.unique()) {
      ++freedPages;
      freedBytes += free->size();
    }
  }
  if (freedPages == 0) {
    VELOX_CHECK_EQ(freedBytes, 0);
    return;
  }
  VELOX_CHECK_GT(freedBytes, 0);

  updateStatsWithFreedPagesLocked(freedPages, freedBytes);

  if (bufferedBytes_ < continueSize_) {
    promises = std::move(promises_);
  }
}

bool OutputBuffer::deleteResults(int destination) {
  std::vector<std::shared_ptr<SerializedPage>> freed;
  std::vector<ContinuePromise> promises;
  bool isFinished;
  DataAvailable dataAvailable;
  {
    std::lock_guard<std::mutex> l(mutex_);
    VELOX_CHECK_LT(destination, buffers_.size());
    auto* buffer = buffers_[destination].get();
    if (buffer == nullptr) {
      VLOG(1) << "Extra delete received for destination " << destination;
      return false;
    }
    freed = buffer->deleteResults();
    dataAvailable = buffer->getAndClearNotify();
    buffer->finish();
    VELOX_CHECK_LT(destination, finishedBufferStats_.size());
    finishedBufferStats_[destination] = buffers_[destination]->stats();
    buffers_[destination] = nullptr;
    ++numFinalAcknowledges_;
    isFinished = isFinishedLocked();
    updateAfterAcknowledgeLocked(freed, promises);
  }

  // Outside of mutex.
  dataAvailable.notify();

  if (!promises.empty()) {
    VLOG(1) << "Delete of results unblocks producers. Can happen in early end "
            << "due to error or limit";
  }
  releaseAfterAcknowledge(freed, promises);
  if (isFinished) {
    task_->setAllOutputConsumed();
  }
  return isFinished;
}

void OutputBuffer::getData(
    int destination,
    uint64_t maxBytes,
    int64_t sequence,
    DataAvailableCallback notify,
    DataConsumerActiveCheckCallback activeCheck) {
  DestinationBuffer::Data data;
  std::vector<std::shared_ptr<SerializedPage>> freed;
  std::vector<ContinuePromise> promises;
  {
    std::lock_guard<std::mutex> l(mutex_);

    if (!isPartitioned() && destination >= buffers_.size()) {
      addOutputBuffersLocked(destination + 1);
    }

    VELOX_CHECK_LT(destination, buffers_.size());
    auto* buffer = buffers_[destination].get();
    VELOX_CHECK_NOT_NULL(
        buffer,
        "getData received after its buffer is deleted. Destination: {}, sequence: {}",
        destination,
        sequence);
    freed = buffer->acknowledge(sequence, true);
    updateAfterAcknowledgeLocked(freed, promises);
    data = buffer->getData(
        maxBytes, sequence, notify, activeCheck, arbitraryBuffer_.get());
  }
  releaseAfterAcknowledge(freed, promises);
  if (data.immediate) {
    notify(std::move(data.data), sequence, std::move(data.remainingBytes));
  }
}

void OutputBuffer::terminate() {
  VELOX_CHECK(!task_->isRunning());

  std::vector<ContinuePromise> outstandingPromises;
  {
    std::lock_guard<std::mutex> l(mutex_);
    outstandingPromises.swap(promises_);
  }
  for (auto& promise : outstandingPromises) {
    promise.setValue();
  }
}

std::string OutputBuffer::toString() {
  std::lock_guard<std::mutex> l(mutex_);
  return toStringLocked();
}

std::string OutputBuffer::toStringLocked() const {
  std::stringstream out;
  out << "[OutputBuffer[" << kind_ << "] bufferedBytes_=" << bufferedBytes_
      << "b, num producers blocked=" << promises_.size()
      << ", completed=" << numFinished_ << "/" << numDrivers_ << ", "
      << (atEnd_ ? "at end, " : "") << "destinations: " << std::endl;
  for (auto i = 0; i < buffers_.size(); ++i) {
    auto buffer = buffers_[i].get();
    out << i << ": " << (buffer ? buffer->toString() : "none") << std::endl;
  }
  if (isArbitrary()) {
    out << arbitraryBuffer_->toString();
  }
  out << "]" << std::endl;
  return out.str();
}

double OutputBuffer::getUtilization() const {
  return bufferedBytes_ / (double)maxSize_;
}

bool OutputBuffer::isOverutilized() const {
  return (bufferedBytes_ > (0.5 * maxSize_)) || atEnd_;
}

int64_t OutputBuffer::getAverageBufferTimeMsLocked() const {
  if (numOutputBytes_ > 0) {
    return totalBufferedBytesMs_ / numOutputBytes_;
  }

  return 0;
}

namespace {

// Find out how many buffers hold 80% of the data. Useful to identify skew.
int32_t countTopBuffers(
    const std::vector<DestinationBuffer::Stats>& bufferStats,
    int64_t totalBytes) {
  std::vector<int64_t> bufferSizes;
  bufferSizes.reserve(bufferStats.size());
  for (auto i = 0; i < bufferStats.size(); ++i) {
    const auto& stats = bufferStats[i];
    bufferSizes.push_back(stats.bytesBuffered + stats.bytesSent);
  }

  // Sort descending.
  std::sort(bufferSizes.begin(), bufferSizes.end(), std::greater<int64_t>());

  const auto limit = totalBytes * 0.8;
  int32_t numBuffers = 0;
  int32_t runningTotal = 0;
  for (auto size : bufferSizes) {
    runningTotal += size;
    numBuffers++;

    if (runningTotal >= limit) {
      break;
    }
  }

  return numBuffers;
}

} // namespace

OutputBuffer::Stats OutputBuffer::stats() {
  std::lock_guard<std::mutex> l(mutex_);
  std::vector<DestinationBuffer::Stats> bufferStats;
  VELOX_CHECK_EQ(buffers_.size(), finishedBufferStats_.size());
  bufferStats.resize(buffers_.size());
  for (auto i = 0; i < buffers_.size(); ++i) {
    auto buffer = buffers_[i].get();
    if (buffer != nullptr) {
      bufferStats[i] = buffer->stats();
    } else {
      bufferStats[i] = finishedBufferStats_[i];
    }
  }

  updateTotalBufferedBytesMsLocked();

  return OutputBuffer::Stats(
      kind_,
      noMoreBuffers_,
      atEnd_,
      isFinishedLocked(),
      bufferedBytes_,
      bufferedPages_,
      numOutputBytes_,
      numOutputRows_,
      numOutputPages_,
      getAverageBufferTimeMsLocked(),
      countTopBuffers(bufferStats, numOutputBytes_),
      bufferStats);
}

} // namespace facebook::velox::exec
