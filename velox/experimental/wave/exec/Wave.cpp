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

OperandId pathToOperand(
    const DefinesMap& map,
    std::vector<std::unique_ptr<common::Subfield::PathElement>>& path) {
  if (path.empty()) {
    return kNoOperand;
  }
  common::Subfield field(std::move(path));
  const auto subfieldMap = threadSubfieldMap();
  auto it = threadSubfieldMap()->find(field.toString());
  if (it == subfieldMap->end()) {
    return kNoOperand;
  }
  Value value(it->second.get());
  auto valueIt = map.find(value);
  path = std::move(field.path());
  if (valueIt == map.end()) {
    return kNoOperand;
  }
  return valueIt->second->id;
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
  // TODO: wait for device side work to finish before freeing associated memory
  // owned by exes and buffers in 'this'.
  for (auto& exe : executables_) {
    if (exe->releaser) {
      exe->releaser(exe);
    }
  }
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
  exe->deviceData.push_back(operands);
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
  for (auto& [ids, exeVector] : dependences) {
    folly::Range<Executable**> exes(exeVector.data(), exeVector.size());
    std::vector<Stream*> required;
    ids.forEach([&](int32_t id) { required.push_back(streams_[id].get()); });
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

template <typename T, typename U>
T addBytes(U* p, int32_t bytes) {
  return reinterpret_cast<T>(reinterpret_cast<uintptr_t>(p) + bytes);
}

LaunchControl* WaveStream::prepareProgramLaunch(
    int32_t key,
    int32_t inputRows,
    folly::Range<Executable**> exes,
    int32_t blocksPerExe,
    bool initStatus,
    Stream* stream) {
  static_assert(Operand::kPointersInOperand * sizeof(void*) == sizeof(Operand));
  int32_t shared = 0;

  //  First calculate total size.
  // 2 int arrays: blockBase, programIdx.
  int32_t numBlocks = std::min<int32_t>(1, exes.size()) * blocksPerExe;
  int32_t size = 2 * numBlocks * sizeof(int32_t);
  auto exeOffset = size;
  // 2 pointers per exe: TB program and start of its param array.
  size += exes.size() * sizeof(void*) * 2;
  auto operandOffset = size;
  // Exe dependent sizes for parameters.
  int32_t numTotalOps = 0;
  for (auto& exe : exes) {
    markLaunch(*stream, *exe);
    shared = std::max(shared, exe->programShared->sharedMemorySize());
    int32_t numIn = exe->inputOperands.size();
    int numOps = numIn + exe->intermediates.size() + exe->outputOperands.size();
    numTotalOps += numOps;
    size += numOps * sizeof(void*) + (numOps - numIn) * sizeof(Operand);
  }
  int32_t statusOffset = 0;
  if (initStatus) {
    statusOffset = size;
    //  Pointer to return block for each tB.
    size += blocksPerExe * sizeof(BlockStatus);
  }
  auto buffer = arena_.allocate<char>(size);

  auto controlUnique = std::make_unique<LaunchControl>();
  auto& control = *controlUnique;

  control.key = key;
  control.inputRows = inputRows;
  control.sharedMemorySize = shared;
  // Now we fill in the various arrays and put their start addresses in
  // 'control'.
  auto start = buffer->as<int32_t>();
  control.blockBase = start;
  control.programIdx = start + numBlocks;
  control.programs = addBytes<ThreadBlockProgram**>(
      control.programIdx, numBlocks * sizeof(int32_t));
  control.operands =
      addBytes<Operand***>(control.programs, exes.size() * sizeof(void*));
  int32_t fill = 0;
  Operand** operandPtrBegin = addBytes<Operand**>(start, operandOffset);
  Operand* operandArrayBegin =
      addBytes<Operand*>(operandPtrBegin, numTotalOps * sizeof(void*));
  if (initStatus) {
    // If the launch produces new statuses (as opposed to updating status of a
    // previous launch), there is an array with a status for each TB. If there
    // are multiple exes, they all share the same error codes. A launch can have
    // a single cardinality change, which will update the row counts in each TB.
    // Writing errors is not serialized but each lane with at least one error
    // will show one error.
    control.status = addBytes<BlockStatus*>(start, statusOffset);
    memset(control.status, 0, blocksPerExe * sizeof(BlockStatus));
    for (auto i = 0; i < blocksPerExe; ++i) {
      auto status = &control.status[i];
      status->numRows =
          i == blocksPerExe - 1 ? inputRows % kBlockSize : kBlockSize;
    }
  } else {
    control.status = nullptr;
  }
  for (auto exeIdx = 0; exeIdx < exes.size(); ++exeIdx) {
    auto exe = exes[exeIdx];
    int32_t numIn = exe->inputOperands.size();
    int32_t numLocal = exe->intermediates.size() + exe->outputOperands.size();
    control.programs[exeIdx] = exe->program;
    control.operands[exeIdx] = operandPtrBegin;
    // We get the actual input operands for the exe from the exes this depends
    // on
    exe->inputOperands.forEach([&](int32_t id) {
      auto* inputExe = operandToExecutable_[id];
      int32_t ordinal = inputExe->outputOperands.ordinal(id);
      *operandPtrBegin = &inputExe->operands[ordinal];
      ++operandPtrBegin;
    });
    // We install the intermediates and outputs from the WaveVectors in the exe.
    exe->operands = operandArrayBegin;
    for (auto& vec : exe->intermediates) {
      *operandPtrBegin = operandArrayBegin;
      vec->toOperand(operandArrayBegin);
      ++operandPtrBegin;
      ++operandArrayBegin;
    }
    for (auto& vec : exe->output) {
      *operandPtrBegin = operandArrayBegin;
      vec->toOperand(operandArrayBegin);
      ++operandPtrBegin;
      ++operandArrayBegin;
    }
    for (auto tbIdx = 0; tbIdx < blocksPerExe; ++tbIdx) {
      control.blockBase[fill] = exeIdx * blocksPerExe;
      control.programIdx[fill] = exeIdx;
    }
  }
  control.deviceData = std::move(buffer);
  launchControl_[key].push_back(std::move(controlUnique));
  return &control;
}

void WaveStream::getOutput(
    folly::Range<const OperandId*> operands,
    WaveVectorPtr* waveVectors) {
  for (auto i = 0; i < operands.size(); ++i) {
    auto id = operands[i];
    auto exe = operandExecutable(id);
    VELOX_CHECK_NOT_NULL(exe);
    auto ordinal = exe->outputOperands.ordinal(id);
    waveVectors[i] = std::move(exe->output[ordinal]);
    if (waveVectors[i] == nullptr) {
      exe->ensureLazyArrived(operands);
      waveVectors[i] = std::move(exe->output[ordinal]);
      VELOX_CHECK_NOT_NULL(waveVectors[i]);
    }
  }
}

ScalarType typeKindCode(TypeKind kind) {
  switch (kind) {
    case TypeKind::BIGINT:
      return ScalarType::kInt64;
    default:
      VELOX_UNSUPPORTED("Bad TypeKind {}", kind);
  }
}

void Program::prepareForDevice(GpuArena& arena) {
  int32_t codeSize = 0;
  int32_t sharedMemorySize = 0;
  for (auto& instruction : instructions_)
    switch (instruction->opCode) {
      case OpCode::kPlus: {
        auto& bin = instruction->as<AbstractBinary>();
        markInput(bin.left);
        markInput(bin.right);
        markResult(bin.result);
        markInput(bin.predicate);
        codeSize += sizeof(Instruction);
        break;
      }
      default:
        VELOX_UNSUPPORTED(
            "OpCode {}", static_cast<int32_t>(instruction->opCode));
    }
  sortSlots();
  arena_ = &arena;
  deviceData_ = arena.allocate<char>(
      codeSize + instructions_.size() * sizeof(void*) +
      sizeof(ThreadBlockProgram));
  program_ = deviceData_->as<ThreadBlockProgram>();
  auto instructionArray = addBytes<Instruction**>(program_, sizeof(*program_));
  program_->sharedMemorySize = sharedMemorySize;
  program_->numInstructions = instructions_.size();
  program_->instructions = instructionArray;
  Instruction* space = addBytes<Instruction*>(
      instructionArray, instructions_.size() * sizeof(void*));
  for (auto& instruction : instructions_) {
    *instructionArray = space;
    ++instructionArray;
    switch (instruction->opCode) {
      case OpCode::kPlus: {
        auto& bin = instruction->as<AbstractBinary>();
        auto typeCode = typeKindCode(bin.left->type->kind());
        // Comstructed on host, no vtable.
        space->opCode = OP_MIX(instruction->opCode, typeCode);
        new (&space->_.binary) IBinary();
        space->_.binary.left = operandIndex(bin.left);
        space->_.binary.right = operandIndex(bin.right);
        space->_.binary.result = operandIndex(bin.result);
        ++space;
        break;
      }
      default:
        VELOX_UNSUPPORTED("Bad OpCode");
    }
  }
}

void Program::sortSlots() {
  // Assigns offsets to input and local/output slots so that all
  // input is first and output next and within input and output, the
  // slots are ordered with lower operand id first. So, if inputs
  // are slots 88 and 22 and outputs are 77 and 33, then the
  // complete order is 22, 88, 33, 77.
  std::vector<AbstractOperand*> ids;
  for (auto& pair : input_) {
    ids.push_back(pair.first);
  }
  std::sort(
      ids.begin(),
      ids.end(),
      [](AbstractOperand*& left, AbstractOperand*& right) {
        return left->id < right->id;
      });
  for (auto i = 0; i < ids.size(); ++i) {
    input_[ids[i]] = i;
  }
  ids.clear();
  for (auto& pair : local_) {
    ids.push_back(pair.first);
  }
  std::sort(
      ids.begin(),
      ids.end(),
      [](AbstractOperand*& left, AbstractOperand*& right) {
        return left->id < right->id;
      });
  for (auto i = 0; i < ids.size(); ++i) {
    local_[ids[i]] = i + input_.size();
  }
}

OperandIndex Program::operandIndex(AbstractOperand* op) const {
  auto it = input_.find(op);
  if (it != input_.end()) {
    return it->second;
  }
  it = local_.find(op);
  if (it == local_.end()) {
    VELOX_FAIL("Bad operand, offset not known");
  }
  return it->second;
}

void Program::markInput(AbstractOperand* op) {
  if (!op) {
    return;
  }
  if (!local_.count(op)) {
    input_[op] = input_.size();
  }
}

void Program::markResult(AbstractOperand* op) {
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
      exe->outputOperands.add(pair.first->id);
    }
    exe->output.resize(local_.size());
    exe->releaser = [](std::unique_ptr<Executable>& ptr) {
      auto program = ptr->programShared.get();
      ptr->reuse();
      program->releaseExe(std::move(ptr));
    };

  } // We have an exe, whether new or reused. Check the vectors.
  int32_t nth = 0;
  exe->outputOperands.forEach([&](int32_t id) {
    ensureWaveVector(
        exe->output[nth], operands[id]->type, maxRows, true, *arena_);
    ++nth;
  });
  return exe;
}
} // namespace facebook::velox::wave
