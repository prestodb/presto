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

#include "velox/experimental/wave/exec/Wave.h"

namespace facebook::velox::wave {

WaveStream::~WaveStream() {
  for (auto& stream : streams_) {
    releaseStream(std::move(stream));
  }
  for (auto& event : allEvents_) {
    std::unique_ptr<Event> temp(event);
    releaseEvent(std::move(temp));
  }
}

std::mutex WaveStream::reserveMutex_;
std::vector<std::unique_ptr<Stream>> WaveStream::streamsForReuse_;
std::vector<std::unique_ptr<Event>> WaveStream::eventsForReuse_;
bool WaveStream::exitInited_{false};

Stream* WaveStream::newStream() {
  auto stream = streamFromReserve();
  auto id = streams_.size();
  stream->userData() = reinterpret_cast<void*>(id);
  auto result = stream.get();
  streams_.push_back(std::move(stream));
  lastEvent_.push_back(nullptr);
  return result;
}

// static
void WaveStream::clearReusable() {
  streamsForReuse_.clear();
  eventsForReuse_.clear();
}

// static
std::unique_ptr<Stream> WaveStream::streamFromReserve() {
  std::lock_guard<std::mutex> l(reserveMutex_);
  if (streamsForReuse_.empty()) {
    auto result = std::make_unique<Stream>();
    if (!exitInited_) {
      // Register handler for clearing resources after first call of API.
      exitInited_ = true;
      atexit(WaveStream::clearReusable);
    }

    return result;
  }
  auto item = std::move(streamsForReuse_.back());
  streamsForReuse_.pop_back();
  return item;
}

//  static
void WaveStream::releaseStream(std::unique_ptr<Stream>&& stream) {
  std::lock_guard<std::mutex> l(reserveMutex_);
  streamsForReuse_.push_back(std::move(stream));
}
Event* WaveStream::newEvent() {
  auto event = eventFromReserve();
  auto result = event.release();
  allEvents_.insert(result);
  return result;
}

// static
std::unique_ptr<Event> WaveStream::eventFromReserve() {
  std::lock_guard<std::mutex> l(reserveMutex_);
  if (eventsForReuse_.empty()) {
    return std::make_unique<Event>();
  }
  auto item = std::move(eventsForReuse_.back());
  eventsForReuse_.pop_back();
  return item;
}

//  static
void WaveStream::releaseEvent(std::unique_ptr<Event>&& event) {
  std::lock_guard<std::mutex> l(reserveMutex_);
  eventsForReuse_.push_back(std::move(event));
}

namespace {
// Copies from pageable host to unified address. Multithreaded memcpy is
// probably best.
void copyData(std::vector<Transfer>& transfers) {
  // TODO: Put memcpys or ppieces of them on AsyncSource if large enough.
  for (auto& transfer : transfers) {
    ::memcpy(transfer.to, transfer.from, transfer.size);
  }
}
} // namespace

void Executable::startTransfer(
    OperandSet outputOperands,
    WaveBufferPtr&& operands,
    std::vector<WaveVectorPtr>&& outputVectors,
    std::vector<Transfer>&& transfers,
    WaveStream& waveStream) {
  auto exe = std::make_unique<Executable>();
  exe->outputOperands = outputOperands;
  exe->output = std::move(outputVectors);
  exe->transfers = std::move(transfers);
  exe->deviceData = operands;
  exe->operands = operands->as<Operand>();
  exe->outputOperands = outputOperands;
  copyData(exe->transfers);
  auto* device = waveStream.device();
  waveStream.installExecutables(
      folly::Range(&exe, 1),
      [&](Stream* stream, folly::Range<Executable**> executables) {
        for (auto& transfer : executables[0]->transfers) {
          stream->prefetch(device, transfer.to, transfer.size);
        }
        waveStream.markLaunch(*stream, *executables[0]);
      });
}

void WaveStream::installExecutables(
    folly::Range<std::unique_ptr<Executable>*> executables,
    std::function<void(Stream*, folly::Range<Executable**>)> launch) {
  folly::F14FastMap<
      OperandSet,
      std::vector<Executable*>,
      OperandSetHasher,
      OperandSetComparer>
      dependences;
  for (auto& exeUnique : executables) {
    executables_.push_back(std::move(exeUnique));
    auto exe = executables_.back().get();
    VELOX_CHECK(exe->stream == nullptr);
    OperandSet streamSet;
    exe->inputOperands.forEach([&](int32_t id) {
      auto* source = operandToExecutable_[id];
      VELOX_CHECK(source != nullptr);
      auto stream = source->stream;
      if (stream) {
        // Compute pending, mark depenedency.
        auto sid = reinterpret_cast<uintptr_t>(stream->userData());
        streamSet.add(sid);
      }
    });
    dependences[streamSet].push_back(exe);
    exe->outputOperands.forEach([&](int32_t id) {
      VELOX_CHECK_EQ(0, operandToExecutable_.count(id));
      operandToExecutable_[id] = exe;
    });
  }

  // exes with no dependences go on a new stream. Streams with dependent compute
  // get an event. The dependent computes ggo on new streams that first wait for
  // the events.
  folly::F14FastMap<int32_t, Event*> streamEvents;
  for (auto& pair : dependences) {
    std::vector<Stream*> required;
    pair.first.forEach(
        [&](int32_t id) { required.push_back(streams_[id].get()); });
    Executable** start = pair.second.data();
    int32_t count = pair.second.size();
    auto exes = folly::range(start, start + count);
    if (required.empty()) {
      auto stream = newStream();
      launch(stream, exes);
    } else {
      for (auto* req : required) {
        auto id = reinterpret_cast<uintptr_t>(req->userData());
        if (streamEvents.count(id) == 0) {
          auto event = newEvent();
          lastEvent_[id] = event;
          event->record(*req);
          streamEvents[id] = event;
        }
      }
      auto launchStream = newStream();
      pair.first.forEach(
          [&](int32_t id) { streamEvents[id]->wait(*launchStream); });
      launch(launchStream, exes);
    }
  }
}

bool WaveStream::isArrived(
    const OperandSet& ids,
    int32_t sleepMicro,
    int32_t timeoutMicro) {
  OperandSet waitSet;
  ids.forEach([&](int32_t id) {
    auto exe = operandToExecutable_[id];
    VELOX_CHECK_NOT_NULL(exe);
    if (!exe->stream) {
      return;
    }
    auto streamId = reinterpret_cast<uintptr_t>(exe->stream->userData());
    if (!lastEvent_[streamId]) {
      lastEvent_[streamId] = newEvent();
      lastEvent_[streamId]->record(*exe->stream);
    }
    if (lastEvent_[streamId]->query()) {
      return;
    }
    waitSet.add(streamId);
  });
  if (waitSet.empty()) {
    return true;
  }
  if (sleepMicro == -1) {
    return false;
  }
  auto start = getCurrentTimeMicro();
  int64_t elapsed = 0;
  while (timeoutMicro == 0 || elapsed < timeoutMicro) {
    bool ready = true;
    waitSet.forEach([&](int32_t id) {
      if (!lastEvent_[id]->query()) {
        ready = false;
      }
    });
    if (ready) {
      return true;
    }
    std::this_thread::sleep_for(std::chrono::microseconds(sleepMicro));
    elapsed = getCurrentTimeMicro() - start;
  }
  return false;
}

} // namespace facebook::velox::wave
