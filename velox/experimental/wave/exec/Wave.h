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

#pragma once

#include "velox/experimental/wave/exec/Instruction.h"
#include "velox/experimental/wave/exec/OperandSet.h"

#include "velox/expression/Expr.h"
#include "velox/type/Subfield.h"

#include "velox/experimental/wave/common/GpuArena.h"
#include "velox/experimental/wave/exec/ExprKernel.h"
#include "velox/experimental/wave/exec/ExprKernelStream.h"
#include "velox/experimental/wave/vector/WaveVector.h"

#include <folly/executors/CPUThreadPoolExecutor.h>

#include <iostream>
#include <utility>

DECLARE_bool(wave_timing);
DECLARE_bool(wave_transfer_timing);
DECLARE_bool(wave_trace_stream);

namespace facebook::velox::wave {

template <typename StreamType, typename MsgType>
inline void TR(StreamType&& stream, const MsgType& msg) {
  if (FLAGS_wave_trace_stream) {
    std::cout << fmt::format(
        "St{}: {}\n", std::forward<StreamType>(stream)->streamIdx(), msg);
  }
}

template <typename MsgType>
inline void TR(const MsgType& msg) {
  if (FLAGS_wave_trace_stream) {
    std::cout << msg;
  }
}

/// Scoped guard, prints the time spent inside if
class PrintTime {
 public:
  PrintTime(const char* title);
  ~PrintTime();

  void setComment(std::string comment) {
    comment_ = std::move(comment);
  }

 private:
  const char* title_;
  uint64_t start_;
  std::string comment_;
};

/// A host side time point for measuring wait and launch prepare latency. Counts
/// both wall microseconds and clocks.
struct WaveTime {
  size_t micros{0};
  uint64_t clocks{0};

  static uint64_t getMicro() {
    return FLAGS_wave_timing ? getCurrentTimeMicro() : 0;
  }

  static WaveTime now() {
    if (!FLAGS_wave_timing) {
      return {0, 0};
    }
    return {getCurrentTimeMicro(), folly::hardware_timestamp()};
  }

  WaveTime operator-(const WaveTime right) const {
    return {micros - right.micros, clocks - right.clocks};
  }

  WaveTime operator+(const WaveTime right) const {
    return {right.micros + micros, right.clocks + clocks};
  }
  void operator+=(const WaveTime& other) {
    micros += other.micros;
    clocks += other.clocks;
  }
  std::string toString() const;
};

class WaveTimer {
 public:
  WaveTimer(WaveTime& accumulator)
      : accumulator_(accumulator), start_(WaveTime::now()) {}
  ~WaveTimer() {
    accumulator_ = accumulator_ + (WaveTime::now() - start_);
  }

 private:
  WaveTime& accumulator_;
  WaveTime start_;
};

struct WaveStats {
  /// Count of WaveStreams.
  int64_t numWaves{0};

  // Count of kernel launches.
  int64_t numKernels{0};

  // Count of thread blocks in all kernel launches.
  int64_t numThreadBlocks{0};

  /// Number of programs. One launch typically has several programs, roughly one
  /// per output column.
  int64_t numPrograms{0};

  /// Number of starting lanes in kernel launches. This is not exactly thread
  /// blocks because the last block per program is not full.
  int64_t numThreads{0};

  /// Data transfer from host to device.
  int64_t bytesToDevice{0};

  int64_t bytesToHost{0};

  /// Number of times the host syncs with device.
  int64_t numSync{0};

  /// Time a host thread runs without activity on device, e.g. after a sync or
  /// before first launch.
  WaveTime hostOnlyTime;
  /// Time a host thread runs after kernel launch preparing the next kernel.
  WaveTime hostParallelTime;
  /// Time a host thread waits for device.
  WaveTime waitTime;

  /// Time a host thread is synchronously staging data to device. This is either
  /// the wall time of multithreaded memcpy to pinned host or the wall time of
  /// multithreaded GPU Direct NVME read. This does not include the time of
  /// hostToDeviceAsync.
  WaveTime stagingTime;

  /// Optionally measured host to device transfer latency.
  WaveTime transferWaitTime;

  void clear();
  void add(const WaveStats& other);
};

// A value a kernel can depend on. Either a dedupped exec::Expr or a dedupped
// subfield. Subfield between operators, Expr inside  an Expr.
struct Value {
  Value() = default;
  Value(const exec::Expr* expr) : expr(expr), subfield(nullptr) {}

  Value(const common::Subfield* subfield) : expr(nullptr), subfield(subfield) {}
  ~Value() = default;

  bool operator==(const Value& other) const {
    // Both exprs and subfields are deduplicated.
    return expr == other.expr && subfield == other.subfield;
  }

  std::string toString() const;

  const exec::Expr* expr;
  const common::Subfield* subfield;
};

struct ValueHasher {
  size_t operator()(const Value& value) const {
    // Hash the addresses because both exprs and subfields are deduplicated.
    return folly::hasher<uint64_t>()(
               reinterpret_cast<uintptr_t>(value.subfield)) ^
        folly::hasher<uint64_t>()(reinterpret_cast<uintptr_t>(value.expr));
  }
};

struct ValueComparer {
  bool operator()(const Value& left, const Value& right) const {
    return left == right;
  }
};

using SubfieldMap =
    folly::F14FastMap<std::string, std::unique_ptr<common::Subfield>>;

using DefinesMap =
    folly::F14FastMap<Value, AbstractOperand*, ValueHasher, ValueComparer>;

/// Translates a set of path steps to an OperandId or kNoOperand if
/// none found. The path is not const because it is temporarily
/// moved into a Subfield. Not thread safe for 'path'.
AbstractOperand* pathToOperand(
    const DefinesMap& map,
    std::vector<std::unique_ptr<common::Subfield::PathElement>>& path);

const SubfieldMap*& threadSubfieldMap();

class WithSubfieldMap {
 public:
  WithSubfieldMap(const SubfieldMap* map) {
    previous_ = threadSubfieldMap();
    threadSubfieldMap() = map;
  }
  ~WithSubfieldMap() {
    threadSubfieldMap() = previous_;
  }

 private:
  const SubfieldMap* previous_;
};

struct Transfer {
  Transfer(const void* from, void* to, size_t size)
      : from(from), to(to), size(size) {}

  const void* from;
  void* to;
  // Transfer size in bytes.
  size_t size;
};

std::string definesToString(const DefinesMap* map);

class WaveStream;
class Program;

/// Represents a device side operator state, like a join/group by hash table or
/// repartition output. Can be scoped to a Task pipeline (all Drivers),
/// WaveStream or to a Program.
struct OperatorState {
  OperatorState() = default;
  OperatorState(std::shared_ptr<GpuArena> arena) : arena(std::move(arena)) {}

  virtual ~OperatorState() = default;

  template <typename T>
  T* as() {
    return reinterpret_cast<T*>(this);
  }

  /// Sets an error. Any thread calling enter() or enterExclusive() will throw
  /// the error. The caller must have successfully called enterExclusive()
  /// first.
  void setError(std::exception_ptr _error) {
    error = _error;
  }

  /// Device readable pointer to the state. If unified memory, should be aligned
  /// to page boundary.
  virtual void* devicePtr() const = 0;

  int32_t id;

  // Arena holding all memory for the resource if the resource is shared between
  // WaveDrivers.
  std::shared_ptr<GpuArena> arena;

  /// Owns the device side data. Starting address of first is passed to the
  /// kernel. Layout depends on operator.
  std::vector<WaveBufferPtr> buffers;

  std::exception_ptr error;
};

struct AggregateOperatorState : public OperatorState {
  AggregateOperatorState(std::shared_ptr<GpuArena> arena)
      : OperatorState(std::move(arena)) {}

  void allocateAggregateHeader(int32_t size, GpuArena& arena);

  /// Sets the sizes in allocators so that the rows run out before the
  /// table is full. In this way there is no need for a separate
  /// rehash check or atomic rehash needed flag.
  void setSizesToSafe();

  void* devicePtr() const override {
    return alignedHead;
  }

  bool isGrouped{false};
  int32_t rowSize;
  int32_t maxReadStreams{1};

  /// Mutex to serialize allocating row ranges to different Drivers in a
  /// multi-driver read.
  std::mutex mutex;

  // 4K aligned header. Must be full pages, pageable in unified memory without
  // affecting surrounding data.
  DeviceAggregation* alignedHead;

  GpuHashTableBase* hashTable{nullptr};

  // Used bytes counting from 'alignedHead'.
  int32_t alignedHeadSize;

  /// True after first created.
  bool isNew{true};

  /// Number of allocators after hash GpuHashTable.
  int32_t numPartitions{1};

  /// Row ranges from filled allocators.
  std::vector<AllocationRange> ranges;

  /// Number of rows in 'ranges'.
  int64_t numRows{0};

  /// Device side bytes in the hash table and rows.
  int64_t bytes{0};

  /// Next range to be prepared for return.
  int32_t rangeIdx{0};

  /// Next row to return.
  int32_t rowIdx{0};

  /// Device side array of per-stream result rows.
  WaveBufferPtr resultRowPointers;

  /// Array of result rows for each streamId.
  std::vector<WaveBufferPtr> resultRows;

  /// A host pinned buffer for copying row pointer arrays to device.
  WaveBufferPtr temp;
};

struct HashTableHolder : public AggregateOperatorState {
  HashTableHolder(std::shared_ptr<GpuArena> arena)
      : AggregateOperatorState(std::move(arena)) {}

  void* devicePtr() const override {
    return hashTable;
  }
};

struct OperatorStateMap {
  std::mutex mutex;
  folly::F14FastMap<int32_t, std::shared_ptr<OperatorState>> states;

  void addIfNew(int32_t id, const std::shared_ptr<OperatorState>& state);
};

/// Represents a kernel or data transfer. Many executables can be in one kernel
/// launch on different thread blocks. Owns the output and intermediate memory
/// for the thread block program or data transfer this represents. Has a
/// WaveStream level unique id for each output column. May be nulllptr if this
/// represents data movement only.
struct Executable {
  virtual ~Executable() = default;

  std::unique_ptr<Executable>
  create(std::shared_ptr<Program> program, int32_t numRows, GpuArena& arena);

  /// Creates a data transfer. The ranges to transfer are associated to this by
  /// addTransfer().
  static void startTransfer(
      OperandSet outputOperands,
      std::vector<WaveVectorPtr>&& outputVectors,
      std::vector<Transfer>&& transfers,
      WaveStream& stream);

  virtual void ensureLazyArrived(folly::Range<const OperandId*> operands) {
    VELOX_UNREACHABLE(
        "A table scan executable is expected to override this "
        "or always produce all columns");
  }

  /// Returns the vector for 'id' or nullptr if does not exist.
  WaveVector* operandVector(OperandId id);

  /// Returns the vector for 'id' and creates an empty vector of 'type' if one
  /// does not exist. The caller will resize.
  WaveVector* operandVector(OperandId id, const TypePtr& type);

  // Clear state to prepare for reuse.
  void reuse() {
    operands = nullptr;
    stream = nullptr;
    wraps.clear();
  }

  virtual std::string toString() const;

  // The containing WaveStream.
  WaveStream* waveStream{nullptr};

  // The Program this is an invocationn of. nullptr if 'this' represents a data
  // transfer or column read.
  std::shared_ptr<Program> programShared;

  // Device memory if not owned by 'programShared_'.
  std::vector<WaveBufferPtr> deviceData;

  // Operand ids for inputs.
  OperandSet inputOperands;

  // Operand ids for local intermediates.
  OperandSet localOperands;

  // Operand ids for outputs.
  OperandSet outputOperands;

  // Unified memory Operand structs for intermediates/outputs/literals. These
  // are a contiguous array of Operand in LaunchControl of 'this'
  Operand* operands;

  // Index of first output operand in 'operands'.
  int32_t firstOutputOperandIdx{-1};

  // Map from wrapAt in AbstractOperand to device side 'indices' with one
  // int32_t* per thread block.
  folly::F14FastMap<int32_t, int32_t**> wraps;

  // Backing memory for intermediate Operands. Free when 'this' arrives. If
  // scheduling follow up work that is synchronized with arrival of 'this', the
  // intermediates can be moved to the dependent executable at time of
  // scheduling.
  std::vector<WaveVectorPtr> intermediates;

  // Backing device memory   for 'output'. These are accessed by dependent
  // executables and must not be written to until out of scope.
  std::vector<WaveVectorPtr> output;

  // If this represents data transfer, the ranges to transfer.
  std::vector<Transfer> transfers;

  // The stream on which this is enqueued. Set by
  // WaveStream::installExecutables(). Cleared after the kernel containing this
  // is seen to realize dependent event.
  Stream* stream{nullptr};

  // Function for returning 'this' to a pool of reusable executables kept by an
  // operator. The function is expected to move the Executable from the
  // std::unique_ptr. Otherwise the Executable will be freed by reset of the
  // unique_ptr.
  std::function<void(std::unique_ptr<Executable>&)> releaser;
};

/// Describes the OperatorStates touched by a Program.
struct ProgramState {
  // Task-wide id.
  int32_t stateId;
  // Function for creating a state. nullptr if the state must exist before
  // creating an executable.
  std::function<std::shared_ptr<OperatorState>(WaveStream& stream)> create;

  /// The instruction using the state. This is where to continue if
  /// the return indicates the instruction is not fully processed.
  int32_t instructionIdx;

  // True if the state is shared across all streams, e.g. hash join build side.
  bool isGlobal{true};
  ///

  /// If non-0, size of device memory scratch area per TB.
  int32_t tempBytesPerTB{0};

  /// If non-0, size of status to return to host for each TB. The device side
  /// address goes via and the host side address goes to the LaunchControl.
  int32_t returnBytesPerTB{0};
};

/// Describes a point to pick up execution of a partially executed program.
struct ContinuePoint {
  ContinuePoint() = default;
  ContinuePoint(int32_t instruction, int32_t rows)
      : instructionIdx(instruction), sourceRows(rows) {}

  bool empty() const {
    return laneMask.empty() && sourceRows == 0;
  }

  /// The index of the instruction where to pick up execution. Must be set if
  /// !this->empty().
  int32_t instructionIdx{-1};

  /// If non-zero, the continue makes up to 'sourceRows' new values.
  int32_t sourceRows{0};
  /// If non-empty, 'sourceRows' must be 0 and  laneMask gives the lanes in
  /// the previous invocation that need to be continued.
  std::vector<uint64_t> laneMask;
};

/// State of one Program in LaunchControl.
struct ProgramLaunch {
  Program* program{nullptr};
  bool isStaged{false};
#if 0
  /// Device side buffer for status returning instructions.
  std::vector<void*> returnBuffers;
  /// Host side address 1:1 to 'returnBuffers'.
  std::vector<void*> hostReturnBuffers;
  /// Device side temp status for instructions.
  std::vector<void*> deviceBuffers;
#endif
  /// Where to continue if previous execution was incomplete. The last advances
  /// first and is popped off.
  AdvanceResult advance;
};

/// Identifies a compiled kernel. The text is the full description
/// of the work. The operator ids are the ids in the plan making the
/// lookup so that every placeholder in the text corresponds to one
/// operand id.
struct ProgramKey {
  std::string text;
  std::vector<AbstractOperand*> input;
  std::vector<AbstractOperand*> local;
  std::vector<AbstractOperand*> output;
};

class Program : public std::enable_shared_from_this<Program> {
 public:
  Program() = default;

  Program(
      OperandSet input,
      OperandSet local,
      OperandSet output,
      OperandSet extraWrap,
      int32_t numBranches,
      int32_t sharedSize,
      const std::vector<std::unique_ptr<AbstractOperand>>& allOperands,
      std::vector<std::unique_ptr<ProgramState>> operatorStates,
      std::unique_ptr<CompiledKernel> kernel);

  void add(std::unique_ptr<AbstractInstruction> instruction) {
    instructions_.push_back(std::move(instruction));
  }

  const std::vector<std::unique_ptr<AbstractInstruction>>& instructions()
      const {
    return instructions_;
  }

  /// Specifies that Operand with 'id' is used by a dependent operation.
  void markOutput(OperandId id) {
    outputIds_.add(id);
  }

  const std::vector<Program*>& dependsOn() const {
    return dependsOn_;
  }

  std::unique_ptr<Executable> getExecutable(
      int32_t maxRows,
      const std::vector<std::unique_ptr<AbstractOperand>>& operands);

  void releaseExe(std::unique_ptr<Executable>&& exe) {
    std::lock_guard<std::mutex> l(mutex_);
    // The exe being freed should not be the last reference to the Program.
    VELOX_CHECK(!exe->programShared.unique());
    exe->programShared = nullptr;
    prepared_.push_back(std::move(exe));
  }

  int32_t sharedMemorySize() const {
    return sharedMemorySize_;
  }

  const folly::F14FastMap<AbstractOperand*, int32_t>& output() const {
    return output_;
  }

  /// Calls pipelineFinished() on instructions.
  void pipelineFinished(WaveStream& stream);

  const std::string& label() const {
    return label_;
  }

  void addLabel(const std::string& label) {
    label_ = label_ + " " + label;
  }

  /// Fills 'ptrs' with device side global/stream states. Creates the states if
  /// necessary.
  void getOperatorStates(WaveStream& stream, std::vector<void*>& ptrs);
  void setExtraWraps(std::vector<AbstractOperand*> operands) {
    extraWraps_ = std::move(operands);
  }

  /// True if begins with a source instruction, like reading and aggregate
  /// result or exchange.
  bool isSource() {
    return !instructions_.empty() &&
        instructions_.front()->opCode == OpCode::kReadAggregate;
  }

  exec::BlockingReason isBlocked(WaveStream& stream, ContinueFuture* future);

  /// If partially executed instructions in the call of 'control',
  /// returns the point where to pick up. If fully executed or not
  /// started, returns the number of rows to obtain from the
  /// source. If no source and no partial execution or source at end
  /// returns empty. If picking up from a partially executed
  /// instruction, sets the lanes to continue in the status of
  /// 'control'.
  AdvanceResult
  canAdvance(WaveStream& stream, LaunchControl* control, int32_t programIdx);

  /// True if last non-return instruction is a sink, e.g. build, repartition. No
  /// output vectors, synced on 'hostReturnEvent_'.
  bool isSink() const;

  /// Records instruction return status. The status is accessed by canAdvance().
  void interpretReturn(
      WaveStream& stream,
      LaunchControl* control,
      int32_t programIdx);

  void registerStatus(WaveStream& stream);

  /// Runs the update callback in 'advance' with the right instruction.  E.g.
  /// rehash device side table,. Caller synchronizes.
  void callUpdateStatus(
      WaveStream& stream,
      const std::vector<WaveStream*>& otherStreams,
      AdvanceResult& result);

  CompiledKernel* kernel() const {
    return kernel_.get();
  }

  OperandSet& extraWrap() {
    return extraWrap_;
  }

  int32_t numBranches() const {
    return numBranches_;
  }

  /// Register that 'entryPointIdx' in 'kernel' manages the state of the
  /// instruction at with 'serial'.
  void addEntryPointForSerial(int32_t serial, int32_t entryPointIdx) {
    serialToEntryPoint_[serial] = entryPointIdx;
  }

  int32_t entryPointIdxBySerial(int32_t serial) {
    auto it = serialToEntryPoint_.find(serial);
    VELOX_CHECK(it != serialToEntryPoint_.end());
    return it->second;
  }

  std::string toString() const;

 private:
  std::unique_ptr<CompiledKernel> kernel_;

  GpuArena* arena_{nullptr};
  std::vector<Program*> dependsOn_;
  DefinesMap produces_;
  std::vector<std::unique_ptr<AbstractInstruction>> instructions_;

  // Input Operand  to offset in operands array.
  folly::F14FastMap<AbstractOperand*, int32_t> input_;

  /// Set of OperandIds for outputs. These must come after intermediates in
  /// Operands array.
  OperandSet outputIds_;

  OperandSet extraWrap_;

  // Local Operand offset in operands array.
  folly::F14FastMap<AbstractOperand*, int32_t> local_;
  // Output Operand offset in operands array.
  folly::F14FastMap<AbstractOperand*, int32_t> output_;

  // OperandIdx for first literal operand.
  int32_t firstLiteralIdx_{-1};

  // Constant Operand  to offset in operands array.
  folly::F14FastMap<AbstractOperand*, int32_t> literal_;

  // Number of distinct code paths in the kernel. The
  int32_t numBranches_{0};

  int32_t sharedMemorySize_{0};

  std::string label_;

  // Serializes 'prepared_'. Access on WaveStrea, is single threaded but sharing
  // Programs across WaveDrivers makes sense, so make the preallocated resource
  // thread safe.
  std::mutex mutex_;

  // a pool of ready to run executables.
  std::vector<std::unique_ptr<Executable>> prepared_;

  // Globals accessed by id from instructions.
  std::vector<std::unique_ptr<ProgramState>> operatorStates_;

  std::vector<AbstractOperand*> extraWraps_;

  // Maps from AbstratcOperator::serial to the per-operator kernel entry point
  // number, e.g. for rehashing a hash table.
  folly::F14FastMap<int32_t, int32_t> serialToEntryPoint_;
};

inline int32_t instructionStatusSize(
    InstructionStatus& status,
    int32_t numBlocks) {
  return bits::roundUp(
      static_cast<uint32_t>(status.gridStateSize) +
          numBlocks * static_cast<uint32_t>(status.blockState),
      8);
}

using ProgramPtr = std::shared_ptr<Program>;

class WaveSplitReader;
struct LaunchControl;

/// Represents consecutive data dependent kernel launches.
class WaveStream {
 public:
  /// Describes what 'this' is doing for purposes of stats collection.
  enum class State {
    // Not runnable, e.g. another WaveStream is being processed by WaveDriver.
    kNotRunning,
    // Running on host only, e.g. preparing for first kernel launch.
    kHost,
    // Running on host with device side work submitted.
    kParallel,
    // Waiting on host thread for device results.
    kWait
  };

  WaveStream(
      std::shared_ptr<GpuArena> arena,
      GpuArena& deviceArena,
      const std::vector<std::unique_ptr<AbstractOperand>>* operands,
      OperatorStateMap* stateMap,
      InstructionStatus state,
      int16_t streamIdx)
      : arena_(std::move(arena)),
        deviceArena_(deviceArena),
        operands_(operands),
        taskStateMap_(stateMap),
        instructionStatus_(state),
        streamIdx_(streamIdx) {
    operandNullable_.resize(operands_->size(), true);
  }

  ~WaveStream();

  // Binds operands of each program to inputs from pending programs and if
  // depending on more than one Wave, adds dependency via events. Each program
  // [i]is dimensioned to have  sizes[i] max intermediates/results.
  void startWave(
      folly::Range<Executable**> programs,
      folly::Range<int32_t*> sizes);

  GpuArena& arena() {
    return *arena_;
  }

  GpuArena& deviceArena() {
    return deviceArena_;
  }

  /// Sets nullability of a source column. This is runtime, since may depend on
  /// the actual presence of nulls in the source, e.g. file. Nullability
  /// defaults to nullable.
  void setNullable(const AbstractOperand& op, bool nullable) {
    operandNullable_[op.id] = nullable;
  }

  int32_t numRows() const {
    return numRows_;
  }

  // Sets the size of top-level vectors to be prepared for the next launch.
  void setNumRows(int32_t numRows) {
    numRows_ = numRows;
  }

  /// Sets 'vector' to ' a WaveVector of suitable type, size and
  /// nullability. May reuse 'vector' if not nullptr. The size comes
  /// from setNumRows() if not given as parameter.
  void ensureVector(
      const AbstractOperand& operand,
      WaveVectorPtr& vector,
      int32_t numRows = -1);

  /// Marks 'op' as being later copied to host.  Allocates these together.
  void markHostOutputOperand(const AbstractOperand& op);

  /// Finalizes return state. setNumRows and markHostOutputOperand may not be
  /// called after this. If 'needStatus' is false and no columns are marked for
  /// host return there is no need for any data transfer at the end of the
  /// stream.
  void setReturnData(bool needStatus);

  /// Enqueus copy of device side results to host.
  void resultToHost();

  /// Updates 'vectors' to reference the data in 'operands'. 'id' is the id of
  /// the last WaveOperator. It identifies the LaunchControl with the final
  /// BlockStatus with errors and cardinalities. Returns the number of rows
  /// after possible selection.
  int32_t getOutput(
      int32_t operatorId,
      memory::MemoryPool& pool,
      folly::Range<const OperandId*> operands,
      VectorPtr* vectors);

  Executable* operandExecutable(OperandId id) {
    auto it = operandToExecutable_.find(id);
    if (it == operandToExecutable_.end()) {
      return nullptr;
    }
    return it->second;
  }

  /// Determines the prerequisites for each of 'executables' and calls
  /// 'launch' for each group of executables with the same
  /// dependencies. 'launch' gets a stream where the prerequisites are
  /// enqueued or a stream on which an event wait for multiple
  /// prerequisites is enqueued for executables with more than one
  /// prerequisite. 'launch' is responsible for enqueuing the actual
  /// kernel or data transfer and marking which stream it went to with
  /// markLaunch(). Takes ownership of 'executables', which are moved out of the
  /// unique_ptrs.
  void installExecutables(
      folly::Range<std::unique_ptr<Executable>*> executables,
      std::function<void(Stream*, folly::Range<Executable**>)> launch);

  /// The callback from installExecutables must call this to establish relation
  /// of stream and executable before returning. Normally, the executable is
  /// launched on the stream given to the callback. In some cases the launch may
  /// decide to use different streams for different executables and have these
  /// depend on the first stream.
  void markLaunch(Stream& stream, Executable& executable) {
    executable.stream = &stream;
  }

  std::vector<bool>& operandNullable() {
    return operandNullable_;
  }

  // Retuns true if all executables needed to cover 'ids' have arrived. if
  // 'sleepMicro' is default, returns immediately if not arrived. Otherwise
  // sleeps 'leepMicros' and rechecks until complete or until 'timeoutMicro' us
  // have elapsed. timeout 0 means wait indefinitely.
  bool isArrived(
      const OperandSet& ids,
      int32_t sleepMicro = -1,
      int32_t timeoutMicro = 0);

  Device* device() const {
    return getDevice();
  }

  /// Returns a new stream, assigns it an id and keeps it owned by 'this'. The
  /// Stream will be returned to the static pool of streams on destruction of
  /// 'this'.
  Stream* newStream();

  static std::unique_ptr<Stream> streamFromReserve();
  static void releaseStream(std::unique_ptr<Stream>&& stream);

  /// Takes ownership of 'buffer' and keeps it until return of all kernels. Used
  /// for keeping working memory passed to kernels live for the duration.
  void addExtraData(int32_t key, WaveBufferPtr buffer) {
    extraData_[key] = std::move(buffer);
  }
  /// Makes a parameter block for multiple program launch. Sends the
  /// data to the device on 'stream' Keeps the record associated with
  /// 'key'. The record contains return status blocks for errors and
  /// row counts. The LaunchControl is in host memory, the arrays
  /// referenced from it are in unified memory, owned by
  /// LaunchControl. 'key' identifies the issuing
  /// WaveOperator. 'nthLaunch' is the serial number of the kernel
  /// within the operator. Multiple launches can have the same serial
  /// for continuing partially executed operations. 'inputRows' is the
  /// logical number of input rows, not all TBs are necessarily
  /// full. 'exes' are the programs launched together, e.g. different
  /// exprs on different columns. 'blocks{PerExe' is the number of TBs
  /// running each exe. 'stream' enqueus the data transfer.
  LaunchControl* prepareProgramLaunch(
      int32_t key,
      int32_t nthlaunch,
      int32_t inputRows,
      folly::Range<Executable**> exes,
      int32_t blocksPerExe,
      const LaunchControl* inputStatus,
      Stream* stream);

  const std::vector<std::unique_ptr<LaunchControl>>& launchControls(
      int32_t key) {
    return launchControl_[key];
  }

  void addLaunchControl(int32_t key, std::unique_ptr<LaunchControl> control) {
    launchControl_[key].push_back(std::move(control));
  }

  void setLaunchControl(
      int32_t key,
      int32_t nth,
      std::unique_ptr<LaunchControl> control);

  const AbstractOperand* operandAt(int32_t id) {
    VELOX_CHECK_LT(id, operands_->size());
    return (*operands_)[id].get();
  }

  // Describes an exe in a multi-program launch.
  struct ExeLaunchInfo {
    int32_t numBlocks;
    int32_t numInput{0};
    int32_t numLocalOps{0};
    int32_t numLocalWrap{0};
    int32_t totalBytes{0};
    folly::F14FastMap<int32_t, int32_t**> inputWrap;
    folly::F14FastMap<int32_t, int32_t**> localWrap;
    int32_t numExtraWrap{0};
    int32_t firstExtraWrap{0};
    std::vector<void*> operatorStates;
  };

  void
  exeLaunchInfo(Executable& exe, int32_t blocksPerExe, ExeLaunchInfo& info);

  Operand** fillOperands(Executable& exe, char* start, ExeLaunchInfo& info);

  State state() const {
    return state_;
  }

  /// Sets the state for stats collection.
  void setState(WaveStream::State state);

  const WaveStats& stats() const {
    return stats_;
  }

  WaveStats& mutableStats() {
    return stats_;
  }

  WaveStats& stats() {
    return stats_;
  }

  void setSplitReader(const std::shared_ptr<WaveSplitReader>& reader) {
    splitReader_ = reader;
  }

  void clearLaunch(int32_t id) {
    launchControl_[id].clear();
  }

  OperatorState* operatorState(int32_t id);

  std::shared_ptr<OperatorState> operatorStateShared(int32_t id);

  OperatorState* newState(ProgramState& init);

  /// Initializes 'state' to the device side state for 'inst'. Returns after
  /// 'state' is ready to use on device.
  void makeAggregate(AbstractAggregation& inst, AggregateOperatorState& state);

  /// Initializes 'state' to the device side state for 'inst'. Returns after
  /// 'state' is ready to use on device.
  void makeHashBuild(AbstractHashBuild& inst, HashTableHolder& state);

  std::unique_ptr<Executable> recycleExecutable(
      Program* program,
      int32_t numRows);

  /// True if ends with a resettable sink like partial aggregation and the sink
  /// is full.
  bool isSinkFull() const {
    return false;
  }

  /// Clears the state in final sink, e.g. partial agregation. The
  /// stream can be continuable at this point and the new sink state
  /// will get the data not produced so far.
  void resetSink() {}

  // Releases and clears streams and events. Done at destruction or
  // before reuse. All device side activity is expected to be
  // complete. Resets conditional nullability info.
  void releaseStreamsAndEvents();

  void setError() {
    TR(this, "Setting error.");
    hasError_ = true;
  }

  static folly::CPUThreadPoolExecutor* copyExecutor();
  static folly::CPUThreadPoolExecutor* syncExecutor();

  std::string toString() const;

  /// Reads the BlockStatus from device and marks programs that need to be
  /// continued.
  bool interpretArrival();

  const InstructionStatus& instructionStatus() const {
    return instructionStatus_;
  }

  /// Returns the grid level return status for instruction with 'status' or
  /// nullptr if no status in place.
  template <typename T>
  T* gridStatus(const InstructionStatus& status) {
    if (!hostBlockStatus_) {
      VELOX_CHECK_NULL(deviceBlockStatus_);
      return nullptr;
    }
    auto numBlocks = bits::roundUp(numRows_, kBlockSize) / kBlockSize;
    return reinterpret_cast<T*>(
        bits::roundUp(
            reinterpret_cast<uintptr_t>(
                &hostBlockStatus_->as<BlockStatus>()[numBlocks]),
            8) +
        status.gridState);
  }

  /// Asynchronously zeroes out the device side  copy of the grid status.
  template <typename T>
  void clearGridStatus(const InstructionStatus& status) {
    if (!deviceBlockStatus_) {
      return;
    }
    auto numBlocks = bits::roundUp(numRows_, kBlockSize) / kBlockSize;
    auto deviceAddress =
        bits::roundUp(
            reinterpret_cast<uintptr_t>(&deviceBlockStatus_[numBlocks]), 8) +
        status.gridState;
    streams_[0]->memset(reinterpret_cast<char*>(deviceAddress), 0, sizeof(T));
  }

  BlockStatus* hostBlockStatus() const {
    return hostBlockStatus_->as<BlockStatus>();
  }

  int16_t streamIdx() const {
    return streamIdx_;
  }

  /// Integrity check for Executables in 'this'.
  void checkExecutables() const;

  /// Integrity check for error codes and row counts in host/device side
  /// statuses.
  void checkBlockStatuses() const;

  /// calls 'action' on the error on 'this' if the error is non-empty.
  void throwIfError(std::function<void(const KernelError*)> action);

  /// Returns the Executable associated with 'this' whose Program contains
  /// 'instruction'. nullptr if not found.
  Executable* executableByInstruction(const AbstractInstruction* instruction);

  OperatorStateMap* taskStateMap() const {
    return taskStateMap_;
  }

  /// Mutable reference to flag indicating that
  bool& mutableExclusiveProcessed() {
    return exclusiveProcessed_;
  }

  const std::shared_ptr<GpuArena>& arenaShared() const {
    return arena_;
  }

 private:
  // true if 'op' is nullable in the context of 'this'.
  bool isNullable(const AbstractOperand& op) const;

  Event* newEvent();

  LaunchControl* lastControl() const;

  void
  makeHashTable(AggregateOperatorState& state, int32_t rowSize, bool makeTable);

  static std::unique_ptr<Event> eventFromReserve();
  static void releaseEvent(std::unique_ptr<Event>&& event);

  // Preallocated Streams and Events.
  static std::mutex reserveMutex_;
  static std::vector<std::unique_ptr<Event>> eventsForReuse_;
  static std::vector<std::unique_ptr<Stream>> streamsForReuse_;
  static bool exitInited_;
  static std::unique_ptr<folly::CPUThreadPoolExecutor> copyExecutor_;
  static std::unique_ptr<folly::CPUThreadPoolExecutor> syncExecutor_;

  static void clearReusable();

  static folly::CPUThreadPoolExecutor* getExecutor(
      std::unique_ptr<folly::CPUThreadPoolExecutor>& ptr);

  // Unified memory.
  std::shared_ptr<GpuArena> arena_;

  // Device memory.
  GpuArena& deviceArena_;

  const std::vector<std::unique_ptr<AbstractOperand>>* const operands_;
  // True at '[i]' if in this stream 'operands_[i]' should have null flags.
  std::vector<bool> operandNullable_;

  // Task-wide states like join hash tables.
  OperatorStateMap* taskStateMap_;

  // Stream level states like small partial aggregates.
  OperatorStateMap streamStateMap_;

  // Space reserved for per-instruction return state above BlockStatus array.
  InstructionStatus instructionStatus_;

  // Identifies 'this' within parallel streams in the same WaveDriver or
  // parallll WaveDrivers in other Driver pipelines.
  const int16_t streamIdx_;

  // Number of rows to allocate for top level vectors for the next kernel
  // launch.
  int32_t numRows_{0};

  folly::F14FastMap<OperandId, Executable*> operandToExecutable_;
  std::vector<std::unique_ptr<Executable>> executables_;

  // Currently active streams, each at the position given by its
  // stream->userData().
  std::vector<std::unique_ptr<Stream>> streams_;

  // The most recent event recorded on the pairwise corresponding element of
  // 'streams_'.
  std::vector<Event*> lastEvent_;
  // If status return copy has been initiated, then this is the event to sync
  // with before accessing the 'hostReturnData_'
  Event* hostReturnEvent_{nullptr};

  // all events recorded on any stream. Events, once seen realized, are moved
  // back to reserve from here.
  folly::F14FastSet<Event*> allEvents_;

  // invocation record with return status blocks for programs. Used for getting
  // errors and filter cardinalities on return of  specific exes.
  folly::F14FastMap<int32_t, std::vector<std::unique_ptr<LaunchControl>>>
      launchControl_;

  folly::F14FastMap<int32_t, WaveBufferPtr> extraData_;

  // ids of operands that need their memory to be in the host return area.
  OperandSet hostOutputOperands_;

  // Offset of the operand in 'hostReturnData_' and 'deviceReturnData_'.
  folly::F14FastMap<OperandId, int64_t> hostReturnOffset_;

  // Size of data returned at end of stream.
  int64_t hostReturnSize_{0};

  int64_t hostReturnDataUsed_{0};

  // Device side data for all returnable data, like BlockStatus and Vector
  // bodies to be copied to host.
  WaveBufferPtr deviceReturnData_;

  // Host pinned memory to which 'deviceReturnData' is copied.
  WaveBufferPtr hostReturnData_;

  // Device/unified pointer to BlockStatus and memory for areas for
  // instructionStatus_. Allocated before first launch and copied to
  // 'hostBlockStatus_' after last kernel in pipeline.
  BlockStatus* deviceBlockStatus_{nullptr};

  // Host side copy of BlockStatus.
  WaveBufferPtr hostBlockStatus_;

  // Time when host side activity last started on 'this'.
  WaveTime start_;

  State state_{State::kNotRunning};

  // set to true if 'this' has an exclusieve section coming and
  // another WaveStream has processed it. If so, the exclusive section
  // of'this' ends without more action and the flag is reset.
  bool exclusiveProcessed_{false};

  bool hasError_{false};

  WaveStats stats_;

  std::shared_ptr<WaveSplitReader> splitReader_;
};

/// Describes all the control data for launching a kernel executing
/// ThreadBlockPrograms. This is a single piece of unified memory with several
/// arrays with one entry per thread block. The arrays are passsed as parameters
/// to the kernel call. The layout is:
///
//// Array of block bases, one per TB. Array of exe indices, one per
//// TB. Arrray of ThreadBlockProgram, one per exe. Number of input
//// operands, one per exe. Array of Operand pointers, one array per
//// exe. Arrray of non input Operands,. The operands array of each
//// exe points here.  This is filled in by host to refer to
//// WaveVectors in each exe. Array of TB return status blocks, one
//// per TB.
struct LaunchControl {
  LaunchControl(int32_t _key, int32_t _inputRows)
      : key(_key), inputRows(_inputRows) {}

  // Id of the initiating operator.
  const int32_t key;

  // Number of rows the programs get as input. Initializes the BlockStatus'es on
  // device in prepareProgamLaunch().
  const int32_t inputRows;

  KernelParams params;

  int32_t sharedMemorySize{0};

  // Storage for all the above in a contiguous unified memory piece.
  WaveBufferPtr deviceData;

  /// Staging for device side temp storage.
  ResultStaging tempStaging;

  /// Staging for device data to be copied to host.
  ResultStaging returnStaging;

  /// Staging for host side buffers that receive the data from 'returnStaging'.
  ResultStaging hostReturnStaging;

  /// Continue info for each Program in the launch.
  std::vector<ProgramLaunch> programInfo;
};

} // namespace facebook::velox::wave
