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
#include "velox/experimental/wave/exec/Vectors.h"

namespace facebook::velox::wave {

std::string WaveTime::toString() const {
  if (micros < 20) {
    return fmt::format("{} ({} clocks)", succinctNanos(micros * 1000), clocks);
  }
  return succinctNanos(micros * 1000);
}

void WaveStats::add(const WaveStats& other) {
  numWaves += other.numWaves;
  numKernels += other.numKernels;
  numThreadBlocks += other.numThreadBlocks;
  numPrograms += other.numPrograms;
  numThreads += other.numThreads;
  numSync += other.numSync;
  bytesToDevice += other.bytesToDevice;
  bytesToHost += other.bytesToHost;
  hostOnlyTime += other.hostOnlyTime;
  hostParallelTime += other.hostParallelTime;
  waitTime += other.waitTime;
}

void WaveStats::clear() {
  new (this) WaveStats();
}

const SubfieldMap*& threadSubfieldMap() {
  thread_local const SubfieldMap* subfields;
  return subfields;
}

std::string definesToString(const DefinesMap* map) {
  std::stringstream out;
  for (const auto& [value, id] : *map) {
    out
        << (value.subfield ? value.subfield->toString()
                           : value.expr->toString(1));
    out << " = " << id->id << " (" << id->type->toString() << ")" << std::endl;
  }
  return out.str();
}

AbstractOperand* pathToOperand(
    const DefinesMap& map,
    std::vector<std::unique_ptr<common::Subfield::PathElement>>& path) {
  if (path.empty()) {
    return nullptr;
  }
  common::Subfield field(std::move(path));
  const auto subfieldMap = threadSubfieldMap();
  auto it = threadSubfieldMap()->find(field.toString());
  if (it == subfieldMap->end()) {
    return nullptr;
  }
  Value value(it->second.get());
  auto valueIt = map.find(value);
  path = std::move(field.path());
  if (valueIt == map.end()) {
    return nullptr;
  }
  return valueIt->second;
}

WaveVector* Executable::operandVector(OperandId id) {
  WaveVectorPtr* ptr = nullptr;
  if (outputOperands.contains(id)) {
    auto ordinal = outputOperands.ordinal(id);
    ptr = &output[ordinal];
  }
  if (localOperands.contains(id)) {
    auto ordinal = localOperands.ordinal(id);
    ptr = &intermediates[ordinal];
  }
  if (*ptr) {
    return ptr->get();
  }
  return nullptr;
}

WaveVector* Executable::operandVector(OperandId id, const TypePtr& type) {
  WaveVectorPtr* ptr = nullptr;
  if (outputOperands.contains(id)) {
    auto ordinal = outputOperands.ordinal(id);
    ptr = &output[ordinal];
  } else if (localOperands.contains(id)) {
    auto ordinal = localOperands.ordinal(id);
    ptr = &intermediates[ordinal];
  } else {
    VELOX_FAIL("No local/output operand found");
  }
  if (*ptr) {
    return ptr->get();
  }
  *ptr = WaveVector::create(type, waveStream->arena());
  return ptr->get();
}

WaveStream::~WaveStream() {
  if (!hasError_) {
    VELOX_CHECK(
        state_ == State::kHost || state_ == State::kNotRunning,
        "Bad state at ~WaveStream: {}",
        static_cast<int32_t>(state_));
  }
  for (auto& stream : streams_) {
    stream->wait();
  }
  for (auto& exe : executables_) {
    if (exe->releaser) {
      exe->releaser(exe);
    }
  }
  releaseStreamsAndEvents();
}

void WaveStream::releaseStreamsAndEvents() {
  for (auto& stream : streams_) {
    releaseStream(std::move(stream));
  }
  for (auto& event : allEvents_) {
    std::unique_ptr<Event> temp(event);
    releaseEvent(std::move(temp));
  }
  allEvents_.clear();
  streams_.clear();
  lastEvent_.clear();
  hostReturnEvent_ = nullptr;
  // Conditional nullability will be set by the source.
  std::fill(operandNullable_.begin(), operandNullable_.end(), true);
}

void WaveStream::setState(WaveStream::State state) {
  if (state == state_) {
    return;
  }
  WaveTime nowTime = WaveTime::now();
  switch (state_) {
    case State::kNotRunning:
      break;
    case State::kHost:
      stats_.hostOnlyTime += nowTime - start_;
      break;
    case State::kParallel:
      stats_.hostParallelTime += nowTime - start_;
      break;
    case State::kWait:
      stats_.waitTime += nowTime - start_;
      break;
  }
  start_ = nowTime;
  state_ = state;
  if (state_ == State::kWait) {
    ++stats_.numSync;
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

OperatorState* WaveStream::operatorState(int32_t id) {
  auto it = taskStateMap_->states.find(id);
  if (it != taskStateMap_->states.end()) {
    return it->second.get();
  }
  return nullptr;
}

OperatorState* WaveStream::newState(ProgramState& init) {
  auto stateShared = init.create(*this);
  taskStateMap_->states[init.stateId] = stateShared;
  return stateShared.get();
}

void WaveStream::markHostOutputOperand(const AbstractOperand& op) {
  hostOutputOperands_.add(op.id);
  auto nullable = isNullable(op);
  auto alignment = WaveVector::alignment(op.type);
  hostReturnSize_ = bits::roundUp(hostReturnSize_, alignment);
  hostReturnSize_ += WaveVector::backingSize(op.type, numRows_, nullable);
}

void WaveStream::setReturnData(bool needStatus) {
  if (!needStatus && hostReturnSize_ == 0) {
    return;
  }
}

void WaveStream::resultToHost() {
  if (streams_.size() == 1) {
    if (hostReturnDataUsed_ > 0) {
      streams_[0]->deviceToHostAsync(
          hostReturnData_->as<char>(),
          deviceReturnData_->as<char>(),
          hostReturnDataUsed_);
    }
    if (!hostReturnEvent_) {
      hostReturnEvent_ = newEvent();
    }
    hostReturnEvent_->record(*streams_[0]);
  } else {
    VELOX_NYI();
  }
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
    std::vector<WaveVectorPtr>&& outputVectors,
    std::vector<Transfer>&& transfers,
    WaveStream& waveStream) {
  auto exe = std::make_unique<Executable>();
  auto numBlocks = bits::roundUp(waveStream.numRows(), kBlockSize) / kBlockSize;
  exe->waveStream = &waveStream;
  exe->outputOperands = outputOperands;
  WaveStream::ExeLaunchInfo info;
  waveStream.exeLaunchInfo(*exe, numBlocks, info);
  exe->output = std::move(outputVectors);
  exe->transfers = std::move(transfers);
  exe->deviceData.push_back(waveStream.arena().allocate<char>(info.totalBytes));
  auto start = exe->deviceData[0]->as<char>();
  exe->operands = waveStream.fillOperands(*exe, start, info)[0];
  copyData(exe->transfers);
  auto* device = waveStream.device();
  waveStream.installExecutables(
      folly::Range(&exe, 1),
      [&](Stream* stream, folly::Range<Executable**> executables) {
        for (auto& transfer : executables[0]->transfers) {
          stream->prefetch(device, transfer.to, transfer.size);
          waveStream.stats().bytesToDevice += transfer.size;
        }
        waveStream.markLaunch(*stream, *executables[0]);
      });
}

std::unique_ptr<Executable> WaveStream::recycleExecutable(
    Program* program,
    int32_t numRows) {
  for (auto i = 0; i < executables_.size(); ++i) {
    if (executables_[i]->programShared.get() == program) {
      auto result = std::move(executables_[i]);
      result->stream = nullptr;
      executables_.erase(executables_.begin() + i);
      return result;
    }
  }
  return nullptr;
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
    exe->waveStream = this;
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
      // The stream may have the same or different exe in place from a previous
      // launch.
      operandToExecutable_[id] = exe;
    });
  }

  // exes with no dependences go on a new stream. Streams with dependent compute
  // get an event. The dependent computes go on new streams that first wait for
  // the events.
  folly::F14FastMap<int32_t, Event*> streamEvents;
  for (auto& [ids, exeVector] : dependences) {
    folly::Range<Executable**> exes(exeVector.data(), exeVector.size());
    std::vector<Stream*> required;
    ids.forEach([&](int32_t id) { required.push_back(streams_[id].get()); });
    if (required.size() == 1) {
      launch(required[0], exes);
      continue;
    }
    if (required.empty()) {
      Stream* stream = nullptr;
      Event* event = nullptr;
      for (auto i = 0; i < streams_.size(); ++i) {
        if (Stream* candidate = streams_[i].get()) {
          VELOX_CHECK_GT(lastEvent_.size(), i);
          if (!lastEvent_[i] || lastEvent_[i]->query()) {
            stream = candidate;
            event = lastEvent_[i];
            break;
          }
        }
      }
      if (!stream) {
        stream = newStream();
      }
      launch(stream, exes);
      if (event) {
        event->record(*stream);
      }
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
      ids.forEach([&](int32_t id) { streamEvents[id]->wait(*launchStream); });
      launch(launchStream, exes);
    }
  }
}

bool WaveStream::isArrived(
    const OperandSet& ids,
    int32_t sleepMicro,
    int32_t timeoutMicro) {
  OperandSet waitSet;
  if (hostReturnEvent_) {
    bool done = hostReturnEvent_->query();
    if (done) {
      releaseStreamsAndEvents();
    }
    return done;
  }
  ids.forEach([&](int32_t id) {
    auto exe = operandToExecutable_[id];
    VELOX_CHECK_NOT_NULL(exe, "No exe produces operand {} in stream", id);
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
    releaseStreamsAndEvents();
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
      releaseStreamsAndEvents();
      return true;
    }
    std::this_thread::sleep_for(std::chrono::microseconds(sleepMicro));
    elapsed = getCurrentTimeMicro() - start;
  }
  return false;
}

void WaveStream::ensureVector(
    const AbstractOperand& op,
    WaveVectorPtr& vector,
    int32_t numRows) {
  if (!vector) {
    vector = std::make_unique<WaveVector>(op.type, arena());
  }
  bool nullable = isNullable(op);
  if (false /*hostOutputOperands_.contains(op.id)*/) {
    VELOX_NYI();
  } else {
    vector->resize(numRows < 0 ? numRows_ : numRows, nullable);
  }
}

bool WaveStream::isNullable(const AbstractOperand& op) const {
  bool notNull = op.notNull;
  if (!notNull) {
    if (op.sourceNullable) {
      notNull = !operandNullable_[op.id];
    } else {
      notNull = true;
      for (auto i : op.nullableIf) {
        if (operandNullable_[i]) {
          notNull = false;
          break;
        }
      }
    }
  }
  return !notNull;
}

void WaveStream::exeLaunchInfo(
    Executable& exe,
    int32_t numBlocks,
    ExeLaunchInfo& info) {
  // The exe has an Operand* for each input/local/output/literal
  // op. It has an Operand for each local/output/literal op. It has
  // an array of numBlock int32_t*'s for every distinct wrapAt in
  // its local/output operands where the wrapAt does not occur in
  // any of the input Operands.
  info.numBlocks = numBlocks;
  info.numInput = exe.inputOperands.size();
  exe.inputOperands.forEach([&](auto id) {
    auto op = operandAt(id);
    auto* inputExe = operandExecutable(op->id);
    if (op->wrappedAt != AbstractOperand::kNoWrap) {
      auto* indices = inputExe->wraps[op->wrappedAt];
      VELOX_CHECK_NOT_NULL(indices);
      info.inputWrap[op->wrappedAt] = indices;
    }
  });

  exe.localOperands.forEach([&](auto id) {
    auto op = operandAt(id);
    if (op->wrappedAt != AbstractOperand::kNoWrap) {
      if (info.inputWrap.find(id) == info.inputWrap.end()) {
        if (info.localWrap.find(op->wrappedAt) == info.localWrap.end()) {
          info.localWrap[op->wrappedAt] = reinterpret_cast<int32_t**>(
              info.localWrap.size() * numBlocks * sizeof(void*));
        }
      }
    }
  });
  exe.outputOperands.forEach([&](auto id) {
    auto op = operandAt(id);
    if (op->wrappedAt != AbstractOperand::kNoWrap) {
      if (info.inputWrap.find(id) == info.inputWrap.end()) {
        if (info.localWrap.find(op->wrappedAt) == info.localWrap.end()) {
          info.localWrap[op->wrappedAt] = reinterpret_cast<int32_t**>(
              info.localWrap.size() * numBlocks * sizeof(void*));
        }
      }
    }
  });
  auto numLiteral = exe.literals ? exe.literals->size() : 0;
  info.numLocalOps =
      exe.localOperands.size() + exe.outputOperands.size() + numLiteral;
  info.totalBytes =
      // Pointer to Operand for input and local Operands.
      sizeof(void*) * (info.numLocalOps + exe.inputOperands.size()) +
      // Flat array of Operand for all but input.
      sizeof(Operand) * info.numLocalOps +
      // Space for the 'indices' for each distinct wrappedAt.
      (info.localWrap.size() * numBlocks * sizeof(void*));
  if (exe.programShared) {
    exe.programShared->getOperatorStates(*this, info.operatorStates);
  }
}

Operand**
WaveStream::fillOperands(Executable& exe, char* start, ExeLaunchInfo& info) {
  Operand** operandPtrBegin = addBytes<Operand**>(start, 0);
  exe.inputOperands.forEach([&](int32_t id) {
    auto* inputExe = operandToExecutable_[id];
    int32_t ordinal = inputExe->outputOperands.ordinal(id);
    *operandPtrBegin =
        &inputExe->operands[inputExe->firstOutputOperandIdx + ordinal];
    ++operandPtrBegin;
  });
  Operand* operandBegin = addBytes<Operand*>(
      start, (info.numInput + info.numLocalOps) * sizeof(void*));
  VELOX_CHECK_EQ(0, reinterpret_cast<uintptr_t>(operandBegin) & 7);
  int32_t* indicesBegin =
      addBytes<int32_t*>(operandBegin, info.numLocalOps * sizeof(Operand));
  for (auto& [id, ptr] : info.localWrap) {
    info.localWrap[id] =
        addBytes<int32_t**>(indicesBegin, reinterpret_cast<int64_t>(ptr));
  }
  exe.wraps = std::move(info.localWrap);
  for (auto& [id, ptr] : info.inputWrap) {
    exe.wraps[id] = ptr;
  }
  exe.intermediates.resize(exe.localOperands.size());
  int32_t fill = 0;
  exe.localOperands.forEach([&](auto id) {
    auto op = operandAt(id);
    ensureVector(*op, exe.intermediates[fill]);
    auto vec = exe.intermediates[fill].get();
    ++fill;
    vec->toOperand(operandBegin);
    if (op->wrappedAt != AbstractOperand::kNoWrap) {
      operandBegin->indices = exe.wraps[op->wrappedAt];
      VELOX_CHECK_NOT_NULL(operandBegin->indices);
    }
    *operandPtrBegin = operandBegin;
    ++operandPtrBegin;
    ++operandBegin;
  });
  exe.firstOutputOperandIdx = exe.intermediates.size();
  exe.output.resize(exe.outputOperands.size());
  fill = 0;
  exe.outputOperands.forEach([&](auto id) {
    auto op = operandAt(id);
    ensureVector(*op, exe.output[fill]);
    auto vec = exe.output[fill].get();
    ++fill;
    vec->toOperand(operandBegin);
    if (op->wrappedAt != AbstractOperand::kNoWrap) {
      operandBegin->indices = exe.wraps[op->wrappedAt];
      VELOX_CHECK_NOT_NULL(operandBegin->indices);
    }
    *operandPtrBegin = operandBegin;
    ++operandPtrBegin;
    ++operandBegin;
  });

  auto numConstants = exe.literals ? exe.literals->size() : 0;
  if (numConstants) {
    memcpy(operandBegin, exe.literals->data(), numConstants * sizeof(Operand));
    for (auto i = 0; i < numConstants; ++i) {
      *operandPtrBegin = operandBegin;
      ++operandPtrBegin;
      ++operandBegin;
    }
  }

  return addBytes<Operand**>(start, 0);
}

LaunchControl* WaveStream::prepareProgramLaunch(
    int32_t key,
    int32_t nthLaunch,
    int32_t inputRows,
    folly::Range<Executable**> exes,
    int32_t blocksPerExe,
    const LaunchControl* inputControl,
    Stream* stream) {
  static_assert(Operand::kPointersInOperand * sizeof(void*) == sizeof(Operand));
  auto& controlVector = launchControl_[key];
  LaunchControl* controlPtr;
  if (controlVector.size() > nthLaunch) {
    controlPtr = controlVector[nthLaunch].get();
  } else {
    controlVector.resize(nthLaunch + 1);
    controlVector[nthLaunch] = std::make_unique<LaunchControl>(key, inputRows);
    controlPtr = controlVector[nthLaunch].get();
  }
  bool isContinue = false;
  auto& control = *controlPtr;
  if (control.programInfo.empty()) {
    control.programInfo.resize(exes.size());
  } else {
    VELOX_CHECK_EQ(exes.size(), control.programInfo.size());
    for (auto& info : control.programInfo) {
      if (info.advance.isRetry) {
        isContinue = true;
        break;
      }
    }
  }
  if (isContinue) {
    VELOX_CHECK_EQ(-1, inputRows);
  } else {
    VELOX_CHECK_LT(0, inputRows);
    numRows_ = inputRows;
  }

  // 2 int arrays: blockBase, programIdx.
  int32_t numBlocks = std::max<int32_t>(1, exes.size()) * blocksPerExe;
  int32_t size = 2 * numBlocks * sizeof(int32_t);
  std::vector<ExeLaunchInfo> info(exes.size());
  auto exeOffset = size;
  // 2 pointers per exe: TB program and start of its param array and 1 int for
  // start PC. Round to 3 for alignment.
  size += exes.size() * sizeof(void*) * 3;
  auto operandOffset = size;
  // Exe dependent sizes for operands.
  int32_t operandBytes = 0;
  int32_t operatorStateBytes = 0;
  int32_t shared = 0;
  for (auto i = 0; i < exes.size(); ++i) {
    exeLaunchInfo(*exes[i], numBlocks, info[i]);
    operandBytes += info[i].totalBytes;
    markLaunch(*stream, *exes[i]);
    shared = std::max(shared, exes[i]->programShared->sharedMemorySize());
    operatorStateBytes += info[i].operatorStates.size() * sizeof(void*);
  }
  size += operandBytes;
  int32_t statusOffset = 0;
  if (!inputControl) {
    statusOffset = size;
    //  Pointer to return block for each tB.
    size += bits::roundUp(blocksPerExe * sizeof(BlockStatus), 8);
  }
  // 1 pointer per exe and an exe-dependent data area.
  int32_t operatorStateOffset = size;
  size += exes.size() * sizeof(void*) + operatorStateBytes;
  auto buffer = arena_.allocate<char>(size);
  // Zero initialization is expected, for example for operands and arrays in
  // Operand::indices.
  memset(buffer->as<char>(), 0, size);

  control.sharedMemorySize = shared;
  // Now we fill in the various arrays and put their start addresses in
  // 'control'.
  auto start = buffer->as<int32_t>();
  control.params.blockBase = start;
  control.params.programIdx = start + numBlocks;
  control.params.programs = addBytes<ThreadBlockProgram**>(
      control.params.programIdx, numBlocks * sizeof(int32_t));
  control.params.operands = addBytes<Operand***>(
      control.params.programs, exes.size() * sizeof(void*));
  control.params.startPC = isContinue
      ? addBytes<int32_t*>(control.params.operands, exes.size() * sizeof(void*))
      : nullptr;

  if (!inputControl) {
    // If the launch produces new statuses (as opposed to updating status of a
    // previous launch), there is an array with a status for each TB. If there
    // are multiple exes, they all share the same error codes. A launch can have
    // a single cardinality change, which will update the row counts in each TB.
    // Writing errors is not serialized but each lane with at least one error
    // will show one error.
    control.params.status = addBytes<BlockStatus*>(start, statusOffset);
    // Memory is already set to all 0.
    for (auto i = 0; i < blocksPerExe; ++i) {
      auto status = &control.params.status[i];
      status->numRows =
          i == blocksPerExe - 1 ? inputRows % kBlockSize : kBlockSize;
    }
  } else {
    control.params.status = inputControl->params.status;
  }
  char* operandStart = addBytes<char*>(start, operandOffset);
  VELOX_CHECK_EQ(0, reinterpret_cast<uintptr_t>(operandStart) & 7);
  int32_t fill = 0;
  for (auto i = 0; i < exes.size(); ++i) {
    control.params.programs[i] = exes[i]->program;
    if (isContinue) {
      control.params.startPC[i] = control.programInfo[i].advance.instructionIdx;
    }
    auto operandPtrs = fillOperands(*exes[i], operandStart, info[i]);
    control.params.operands[i] = operandPtrs;
    // The operands defined by the exe start after the input operands and are
    // all consecutive.
    exes[i]->operands = operandPtrs[exes[i]->inputOperands.size()];
    operandStart += info[i].totalBytes;
    for (auto tbIdx = 0; tbIdx < blocksPerExe; ++tbIdx) {
      control.params.blockBase[fill] = i * blocksPerExe;
      control.params.programIdx[fill] = i;
      ++fill;
    }
  }

  // Fill in operator states, e.g. hash tables.
  void** operatorStatePtrs = addBytes<void**>(start, operatorStateOffset);
  control.params.operatorStates = reinterpret_cast<void***>(operatorStatePtrs);
  auto stateFill = operatorStatePtrs + info.size();
  for (auto i = 0; i < info.size(); ++i) {
    operatorStatePtrs[i] = stateFill;
    auto& ptrs = info[i].operatorStates;
    for (auto j = 0; j < ptrs.size(); ++j) {
      *stateFill = ptrs[j];
      ++stateFill;
    }
  }
  if (!exes.empty()) {
    ++stats_.numKernels;
  }

  stats_.numPrograms += exes.size();
  stats_.numThreadBlocks += blocksPerExe * exes.size();
  stats_.numThreads += numRows_ * exes.size();

  control.deviceData = std::move(buffer);
  return &control;
}

int32_t WaveStream::getOutput(
    int32_t operatorId,
    memory::MemoryPool& pool,
    folly::Range<const OperandId*> operands,
    VectorPtr* vectors) {
  auto it = launchControl_.find(operatorId);
  VELOX_CHECK(it != launchControl_.end());
  auto* control = it->second[0].get();
  auto* status = control->params.status;
  auto numBlocks = bits::roundUp(numRows_, kBlockSize) / kBlockSize;
  if (operands.empty()) {
    return statusNumRows(status, numBlocks);
  }
  for (auto i = 0; i < operands.size(); ++i) {
    auto id = operands[i];
    auto exe = operandExecutable(id);
    VELOX_CHECK_NOT_NULL(exe);
    auto ordinal = exe->outputOperands.ordinal(id);
    auto waveVectorPtr = &exe->output[ordinal];
    if (!waveVectorPtr->get()) {
      exe->ensureLazyArrived(operands);
      VELOX_CHECK_NOT_NULL(
          waveVectorPtr->get(), "Lazy load should have filled in the result");
    }
    vectors[i] = waveVectorPtr->get()->toVelox(
        &pool,
        numBlocks,
        status,
        &exe->operands[exe->firstOutputOperandIdx + ordinal]);
  }
  return vectors[0]->size();
}

void WaveStream::makeAggregate(
    AbstractAggregation& inst,
    AggregateOperatorState& state) {
  VELOX_CHECK(inst.keys.empty());
  int32_t size = inst.rowSize();
  auto stream = streamFromReserve();
  auto buffer = arena_.allocate<char>(size + sizeof(DeviceAggregation));
  state.buffers.push_back(buffer);
  AggregationControl control;
  control.head = buffer->as<char>();
  control.headSize = buffer->size();
  control.rowSize = size;
  reinterpret_cast<WaveKernelStream*>(stream.get())->setupAggregation(control);
  releaseStream(std::move(stream));
}

std::string WaveStream::toString() const {
  std::stringstream out;
  out << "{WaveStream ";
  for (auto& exe : executables_) {
    out << exe->toString() << std::endl;
  }
  out << "}";
  if (hostReturnEvent_) {
    out << fmt::format("hostReturnEvent={}", hostReturnEvent_->query())
        << std::endl;
  }
  for (auto i = 0; i < streams_.size(); ++i) {
    out << fmt::format(
        "stream {} {}, ",
        streams_[i]->userData(),
        lastEvent_[i] ? fmt::format("event={}", lastEvent_[i]->query())
                      : fmt::format("no event"));
  }
  return out.str();
}

WaveTypeKind typeKindCode(TypeKind kind) {
  return static_cast<WaveTypeKind>(kind);
}

void Program::getOperatorStates(WaveStream& stream, std::vector<void*>& ptrs) {
  ptrs.resize(operatorStates_.size());
  for (auto i = 0; i < operatorStates_.size(); ++i) {
    auto& operatorState = *operatorStates_[i];
    auto* state = stream.operatorState(operatorState.stateId);
    if (!state) {
      VELOX_CHECK_NOT_NULL(operatorState.create);
      state = stream.newState(operatorState);
    }
    ptrs[i] = state->buffers[0]->as<char>();
  }
}

bool Program::isSink() const {
  int32_t size = instructions_.size();
  if (instructions_[size - 1]->opCode == OpCode::kReturn) {
    VELOX_CHECK_GE(size, 2);
    return instructions_[size - 2]->isSink();
  }
  return instructions_[size - 1]->isSink();
}

AdvanceResult Program::canAdvance(
    WaveStream& stream,
    LaunchControl* control,
    int32_t programIdx) {
  AbstractInstruction* source = instructions_.front().get();
  OperatorState* state = nullptr;
  auto stateId = source->stateId();
  if (stateId.has_value()) {
    state = stream.operatorState(stateId.value());
  }
  return source->canAdvance(stream, control, state, programIdx);
}

#define IN_HEAD(abstract, physical, _op)             \
  auto* abstractInst = &instruction->as<abstract>(); \
  space->opCode = _op;                               \
  auto physicalInst = new (&space->_) physical();

#define IN_OPERAND(member) \
  physicalInst->member = operandIndex(abstractInst->member)

void Program::prepareForDevice(GpuArena& arena) {
  VELOX_CHECK(!instructions_.empty());
  if (instructions_.back()->opCode != OpCode::kReturn) {
    instructions_.push_back(std::make_unique<AbstractReturn>());
  }
  int32_t codeSize = sizeof(Instruction) * instructions_.size();
  for (auto& instruction : instructions_)
    switch (instruction->opCode) {
      case OpCode::kFilter: {
        auto& filter = instruction->as<AbstractFilter>();
        markInput(filter.flags);
        markResult(filter.indices);

        break;
      }
      case OpCode::kWrap: {
        auto& wrap = instruction->as<AbstractWrap>();
        markInput(wrap.indices);
        std::vector<OperandIndex> indices(wrap.target.size());
        wrap.literalOffset = addLiteral(indices.data(), indices.size());
        for (auto i = 0; i < wrap.target.size(); ++i) {
          auto target = wrap.target[i];
          markInput(wrap.source[i]);
          if (target != wrap.source[i]) {
            markResult(target);
          }
        }
        break;
      }
      case OpCode::kPlus_BIGINT:
      case OpCode::kLT_BIGINT: {
        auto& bin = instruction->as<AbstractBinary>();
        markInput(bin.left);
        markInput(bin.right);
        markResult(bin.result);
        markInput(bin.predicate);
        break;
      }
      case OpCode::kNegate: {
        auto& un = instruction->as<AbstractUnary>();
        markInput(un.input);
        markResult(un.result);
        markInput(un.predicate);
        break;
      }
      case OpCode::kReturn:
        break;
      case OpCode::kAggregate: {
        auto& agg = instruction->as<AbstractAggregation>();
        for (auto& key : agg.keys) {
          markInput(key);
        }
        // The literal area has first IUpdateAggs and then Operandindex's for
        // the keys.
        int32_t extra =
            bits::roundUp(
                agg.keys.size() * sizeof(OperandIndex), sizeof(IUpdateAgg)) /
            sizeof(IUpdateAgg);
        std::vector<IUpdateAgg> temp(agg.aggregates.size() + extra);
        agg.literalOffset = addLiteral(temp.data(), temp.size());
        agg.literalBytes = temp.size() * sizeof(IUpdateAgg);
        for (auto& op : agg.aggregates) {
          for (auto& arg : op.args) {
            markInput(arg);
          }
        }
        break;
      }
      case OpCode::kReadAggregate: {
        auto& read = instruction->as<AbstractReadAggregation>();
        auto& agg = *read.aggregation;
        for (auto& key : agg.keys) {
          markResult(key);
        }
        // The literal area is like in the aggregation.
        int32_t extra = agg.literalBytes;
        std::vector<IUpdateAgg> temp(extra / sizeof(IUpdateAgg));
        read.literalOffset = addLiteral(temp.data(), temp.size());
        for (auto& key : agg.keys) {
          markResult(key);
        }
        for (auto& op : agg.aggregates) {
          markResult(op.result);
        }
        break;
      }
      default:
        VELOX_UNSUPPORTED(
            "OpCode {}", static_cast<int32_t>(instruction->opCode));
    }
  sortSlots();
  arena_ = &arena;
  deviceData_ = arena.allocate<char>(
      codeSize + literalArea_.size() + sizeof(ThreadBlockProgram));
  uintptr_t end = reinterpret_cast<uintptr_t>(
      deviceData_->as<char>() + deviceData_->size());
  program_ = deviceData_->as<ThreadBlockProgram>();
  auto instructionArray = addBytes<Instruction*>(program_, sizeof(*program_));
  program_->numInstructions = instructions_.size();
  program_->instructions = instructionArray;
  Instruction* space = instructionArray;
  deviceLiterals_ = reinterpret_cast<char*>(space) +
      sizeof(Instruction) * instructions_.size();
  VELOX_CHECK_LE(
      reinterpret_cast<uintptr_t>(deviceLiterals_) + literalArea_.size(), end);
  memcpy(deviceLiterals_, literalArea_.data(), literalArea_.size());

  for (auto& instruction : instructions_) {
    switch (instruction->opCode) {
      case OpCode::kPlus_BIGINT:
      case OpCode::kLT_BIGINT: {
        IN_HEAD(AbstractBinary, IBinary, instruction->opCode)

        IN_OPERAND(left);
        IN_OPERAND(right);
        IN_OPERAND(result);
        IN_OPERAND(predicate);
        break;
      }
      case OpCode::kFilter: {
        IN_HEAD(AbstractFilter, IFilter, OpCode::kFilter);
        IN_OPERAND(flags);
        IN_OPERAND(indices);
        break;
      }
      case OpCode::kWrap: {
        IN_HEAD(AbstractWrap, IWrap, OpCode::kWrap);
        IN_OPERAND(indices);
        physicalInst->numColumns = abstractInst->source.size();
        physicalInst->columns = reinterpret_cast<OperandIndex*>(
            deviceLiterals_ + abstractInst->literalOffset);
        for (auto i = 0; i < abstractInst->source.size(); ++i) {
          physicalInst->columns[i] = operandIndex(abstractInst->source[i]);
        }
        break;
      }
      case OpCode::kAggregate: {
        IN_HEAD(AbstractAggregation, IAggregate, OpCode::kAggregate);

        physicalInst->numKeys = abstractInst->keys.size();
        physicalInst->numAggregates = abstractInst->aggregates.size();
        physicalInst->stateIndex = operatorStates_.size();
        auto programState = std::make_unique<ProgramState>();
        programState->stateId = abstractInst->state->id;
        programState->isGlobal = true;
        programState->create =
            [inst = abstractInst](
                WaveStream& stream) -> std::shared_ptr<OperatorState> {
          auto newState = std::make_shared<AggregateOperatorState>();
          newState->instruction = inst;
          stream.makeAggregate(*inst, *newState);
          return newState;
        };
        operatorStates_.push_back(std::move(programState));
        physicalInst->aggregates = reinterpret_cast<IUpdateAgg*>(
            deviceLiterals_ + abstractInst->literalOffset);
        // the literal is copied when making the reader for aggregates.
        abstractInst->literal = physicalInst->aggregates;
        OperandIndex* keys = reinterpret_cast<OperandIndex*>(
            physicalInst->aggregates + physicalInst->numAggregates);
        for (auto i = 0; i < abstractInst->keys.size(); ++i) {
          keys[i] = operandIndex(abstractInst->keys[i]);
        }
        for (auto i = 0; i < abstractInst->aggregates.size(); ++i) {
          auto physicalAgg = physicalInst->aggregates + i;
          auto& abstractAgg = abstractInst->aggregates[i];
          physicalAgg->op = abstractAgg.op;
          physicalAgg->nullOffset = abstractAgg.nullOffset;
          physicalAgg->accumulatorOffset = abstractAgg.accumulatorOffset;
          if (abstractAgg.args.size() > 0) {
            physicalAgg->arg1 = operandIndex(abstractAgg.args[0]);
          }
          if (abstractAgg.args.size() > 1) {
            physicalAgg->arg2 = operandIndex(abstractAgg.args[1]);
          }
        }
        break;
      }
      case OpCode::kReadAggregate: {
        IN_HEAD(AbstractReadAggregation, IAggregate, OpCode::kReadAggregate);
        auto& agg = *abstractInst->aggregation;
        physicalInst->numKeys = agg.keys.size();
        physicalInst->numAggregates = agg.aggregates.size();
        physicalInst->aggregates = reinterpret_cast<IUpdateAgg*>(
            deviceLiterals_ + abstractInst->literalOffset);
        auto programState = std::make_unique<ProgramState>();
        programState->stateId = agg.state->id;
        programState->isGlobal = true;
        physicalInst->stateIndex = operatorStates_.size();
        operatorStates_.push_back(std::move(programState));
        memcpy(physicalInst->aggregates, agg.literal, agg.literalBytes);
        auto numKeys = agg.keys.size();
        auto* keys = reinterpret_cast<OperandIndex*>(
            &physicalInst->aggregates[physicalInst->numAggregates]);
        for (auto i = 0; i < numKeys; ++i) {
          keys[i] = operandIndex(agg.keys[i]);
        }
        for (auto i = 0; i < agg.aggregates.size(); ++i) {
          physicalInst->aggregates[i].result =
              operandIndex(agg.aggregates[i].result);
        }
        break;
      }
      case OpCode::kReturn: {
        IN_HEAD(AbstractReturn, IReturn, OpCode::kReturn);
        break;
      }
      default:
        VELOX_UNSUPPORTED("Bad OpCode");
    }
    sharedMemorySize_ =
        std::max(sharedMemorySize_, instructionSharedMemory(*space));
    ++space;
    VELOX_CHECK_LE(
        reinterpret_cast<uintptr_t>(space),
        reinterpret_cast<uintptr_t>(deviceLiterals_));
  }
  program_->sharedMemorySize = sharedMemorySize_;
  literalOperands_.resize(literal_.size());
  for (auto& [op, index] : literal_) {
    literalToOperand(op, literalOperands_[index - firstLiteralIdx_]);
  }
}

void Program::literalToOperand(AbstractOperand* abstractOp, Operand& op) {
  op.indexMask = 0;
  op.indices = nullptr;
  if (abstractOp->literalNull) {
    op.nulls =
        reinterpret_cast<uint8_t*>(deviceLiterals_ + abstractOp->literalOffset);
  } else {
    op.base = deviceLiterals_ + abstractOp->literalOffset;
  }
}

namespace {
// Sorts 'map' by id. Inserts back into map with second as ordinal number
// starting at 'startAt'. Returns 1 + the highest assigned number.
int32_t sortAndRenumber(
    int32_t startAt,
    folly::F14FastMap<AbstractOperand*, int32_t>& map) {
  std::vector<AbstractOperand*> ids;
  for (auto& pair : map) {
    ids.push_back(pair.first);
  }
  std::sort(
      ids.begin(),
      ids.end(),
      [](AbstractOperand*& left, AbstractOperand*& right) {
        return left->id < right->id;
      });
  for (auto i = 0; i < ids.size(); ++i) {
    map[ids[i]] = i + startAt;
  }
  return startAt + ids.size();
}
} // namespace

void Program::sortSlots() {
  // Assigns offsets to input and local/output slots so that all
  // input is first and output next and within input and output, the
  // slots are ordered with lower operand id first. So, if inputs
  // are slots 88 and 22 and outputs are 77 and 33, then the
  // complete order is 22, 88, 33, 77. Constants are sorted after everything
  // else.

  auto start = sortAndRenumber(0, input_);
  start = sortAndRenumber(start, local_);
  start = sortAndRenumber(start, output_);
  firstLiteralIdx_ = start;
  sortAndRenumber(start, literal_);
}

OperandIndex Program::operandIndex(AbstractOperand* op) const {
  if (!op) {
    return kEmpty;
  }
  auto it = input_.find(op);
  if (it != input_.end()) {
    return it->second;
  }
  it = local_.find(op);
  if (it != local_.end()) {
    return it->second;
  }
  it = output_.find(op);
  if (it != local_.end()) {
    return it->second;
  }

  it = literal_.find(op);
  if (it != literal_.end()) {
    return it->second;
  }
  VELOX_FAIL("Operand not found");
}

template <typename T>
int32_t Program::addLiteral(T* value, int32_t count) {
  nextLiteral_ = bits::roundUp(nextLiteral_, sizeof(T));
  auto start = nextLiteral_;
  nextLiteral_ += sizeof(T) * count;
  literalArea_.resize(nextLiteral_);
  memcpy(literalArea_.data() + start, value, sizeof(T) * count);
  return start;
}

template <TypeKind kind>
int32_t Program::addLiteralTyped(AbstractOperand* op) {
  if (op->literalOffset != AbstractOperand::kNoConstant) {
    return op->literalOffset;
  }
  using T = typename TypeTraits<kind>::NativeType;
  if (op->constant->isNullAt(0)) {
    op->literalNull = true;
    char zero = 0;
    return op->literalOffset = addLiteral<char>(&zero, 1);
  }
  T value = op->constant->as<SimpleVector<T>>()->valueAt(0);
  if constexpr (std::is_same_v<T, StringView>) {
    int64_t inlined = 0;
    StringView* stringView = reinterpret_cast<StringView*>(&value);
    if (stringView->size() <= 6) {
      int64_t inlined = static_cast<int64_t>(stringView->size()) << 48;
      memcpy(
          reinterpret_cast<char*>(&inlined) + 2,
          stringView->data(),
          stringView->size());
      op->literalOffset = addLiteral(&inlined, 1);
    } else {
      int64_t zero = 0;
      op->literalOffset = addLiteral(&zero, 1);
      addLiteral(stringView->data(), stringView->size());
    }
  } else {
    op->literalOffset = addLiteral(&value, 1);
  }
  return op->literalOffset;
}

void Program::markInput(AbstractOperand* op) {
  if (!op) {
    return;
  }
  if (op->constant) {
    VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
        addLiteralTyped, op->constant->type()->kind(), op);
    literal_[op] = literal_.size();
    return;
  }
  if (!local_.count(op) && !output_.count(op)) {
    input_[op] = input_.size();
  }
}

void Program::markResult(AbstractOperand* op) {
  if (outputIds_.contains(op->id)) {
    output_[op] = outputIds_.ordinal(op->id);
    return;
  }
  if (!local_.count(op)) {
    local_[op] = local_.size();
  }
}

std::unique_ptr<Executable> Program::getExecutable(
    int32_t maxRows,
    const std::vector<std::unique_ptr<AbstractOperand>>& operands) {
  std::unique_ptr<Executable> exe;
  {
    std::lock_guard<std::mutex> l(mutex_);
    if (!prepared_.empty()) {
      exe = std::move(prepared_.back());
      prepared_.pop_back();
    }
  }
  if (!exe) {
    exe = std::make_unique<Executable>();
    exe->programShared = shared_from_this();
    exe->program = program_;
    for (auto& pair : input_) {
      exe->inputOperands.add(pair.first->id);
    }
    for (auto& pair : local_) {
      exe->localOperands.add(pair.first->id);
    }
    for (auto& pair : output_) {
      exe->outputOperands.add(pair.first->id);
    }

    exe->literals = &literalOperands_;
    exe->releaser = [](std::unique_ptr<Executable>& ptr) {
      auto program = ptr->programShared.get();
      ptr->reuse();
      program->releaseExe(std::move(ptr));
    };
  }
  return exe;
}

std::string AbstractOperand::toString() const {
  if (constant) {
    return fmt::format(
        "<literal {} {}>", constant->toString(0), type->toString());
  }
  const char* nulls = notNull ? "NN"
      : conditionalNonNull    ? "CN"
      : sourceNullable        ? "SN"
                              : "?";
  return fmt::format("<{} {}: {} {}>", id, label, nulls, type->toString());
}

std::string Executable::toString() const {
  std::stringstream out;
  out << "{Exe "
      << (stream ? fmt::format("stream {}", stream->userData())
                 : fmt::format(" no stream "))
      << " produces ";
  bool first = true;
  outputOperands.forEach([&](auto id) {
    if (!first) {
      out << ", ";
    };
    first = false;
    out << waveStream->operandAt(id)->toString();
  });
  if (programShared) {
    out << std::endl;
    out << "program " << programShared->toString();
  }
  return out.str();
}

std::string Program::toString() const {
  std::stringstream out;
  out << "{ program" << std::endl;
  for (auto& instruction : instructions_) {
    out << instruction->toString() << std::endl;
  }
  out << "}" << std::endl;
  return out.str();
}

std::string AbstractFilter::toString() const {
  return fmt::format("filter {} -> {}", flags->toString(), indices->toString());
  ;
}

std::string AbstractWrap::toString() const {
  std::stringstream out;
  out << "wrap indices=" << indices->toString() << " {";
  for (auto& op : source) {
    out << op->toString() << " ";
  }
  out << "}";
  return out.str();
}

std::string AbstractBinary::toString() const {
  return fmt::format(
      "{} = {} {} {} {}",
      result->toString(),
      left->toString(),
      static_cast<int32_t>(opCode),
      right->toString(),
      predicate ? fmt::format(" if {}", predicate->toString()) : "");
}

} // namespace facebook::velox::wave
