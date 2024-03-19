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
#include "velox/experimental/wave/vector/WaveVector.h"

namespace facebook::velox::wave {

// A value a kernel can depend on. Either a dedupped exec::Expr or a dedupped
// subfield. Subfield between operators, Expr inside  an Expr.
struct Value {
  Value() = default;
  Value(const exec::Expr* expr) : expr(expr), subfield(nullptr) {}

  Value(const common::Subfield* subfield) : expr(nullptr), subfield(subfield) {}
  ~Value() = default;

  bool operator==(const Value& other) const {
    if (expr == other.expr && subfield == other.subfield) {
      return true;
    };
    if (subfield && other.subfield && *subfield == *other.subfield) {
      return true;
    }
    return false;
  }

  const exec::Expr* expr;
  const common::Subfield* subfield;
};

struct ValueHasher {
  size_t operator()(const Value& value) const {
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
OperandId pathToOperand(
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

/// Represents a kernel or data transfer. Many executables can be in one kernel
/// launch on different thread blocks. Owns the output and intermediate memory
/// for the thread block program or data transfer this represents. Has a
/// WaveStream level unique id for each output column. be nulllptr if this
/// represents data movement only.
struct Executable {
  virtual ~Executable() = default;

  std::unique_ptr<Executable>
  create(std::shared_ptr<Program> program, int32_t numRows, GpuArena& arena);

  /// Creates a data transfer. The ranges to transfer are associated to this by
  /// addTransfer().
  static void startTransfer(
      OperandSet outputOperands,
      WaveBufferPtr&& operands,
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
  }
  // The containing WaveStream, if needed.
  WaveStream* waveStream{nullptr};

  // The Program this is an invocationn of. nullptr if 'this' represents a data
  // transfer or column read.
  std::shared_ptr<Program> programShared;

  ThreadBlockProgram* program{nullptr};

  // Device memory if not owned by 'programShared_'.
  std::vector<WaveBufferPtr> deviceData;

  // Operand ids for inputs.
  OperandSet inputOperands;

  // Operand ids for local intermediates.
  OperandSet localOperands;

  // Operand ids for outputs.
  OperandSet outputOperands;

  // Unified memory Operand structs for intermediates/outputs. These
  // are a contiguous array of Operand in LaunchControl of 'this'
  Operand* operands;

  // Backing memory for intermediate Operands. Free when 'this' arrives. If
  // scheduling follow up work that is synchronized with arrival of 'this', the
  // intermediates can be moved to the dependent executable at time of
  // scheduling.
  std::vector<WaveVectorPtr> intermediates;

  // Backing device memory   for 'output' Can be moved to intermediates or
  // output of a dependent executables.
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

class Program : public std::enable_shared_from_this<Program> {
 public:
  void add(std::unique_ptr<AbstractInstruction> instruction) {
    instructions_.push_back(std::move(instruction));
  }

  const std::vector<Program*>& dependsOn() const {
    return dependsOn_;
  }

  void addSource(Program* source) {
    if (std::find(dependsOn_.begin(), dependsOn_.end(), source) !=
        dependsOn_.end()) {
      return;
    }
    dependsOn_.push_back(source);
  }

  // Initializes executableImage and relocation information and places for
  // parameters.
  void prepareForDevice(GpuArena& arena);

  std::unique_ptr<Executable> getExecutable(
      int32_t maxRows,
      const std::vector<std::unique_ptr<AbstractOperand>>& operands);

  ThreadBlockProgram* threadBlockProgram() {
    return program_;
  }

  /// True if instructions can be added.
  bool isMutable() const {
    return isMutable_;
  }

  /// Disallows adding instructions to 'this'. For example, a program in an
  /// operator before a cardinality chaning operator cannot get more
  /// instructions from code after the cardinality change.
  void freeze() {
    isMutable_ = false;
  }

  void releaseExe(std::unique_ptr<Executable>&& exe) {
    prepared_.push_back(std::move(exe));
  }

  int32_t sharedMemorySize() const {
    return sharedMemorySize_;
  }

  const folly::F14FastMap<AbstractOperand*, int32_t>& localAndOutput() const {
    return local_;
  }

 private:
  GpuArena* arena_{nullptr};
  std::vector<Program*> dependsOn_;
  DefinesMap produces_;
  std::vector<std::unique_ptr<AbstractInstruction>> instructions_;
  bool isMutable_{true};

  // Adds 'op' to 'input' if it is not produced by one in 'local'
  void markInput(AbstractOperand* op);
  // Adds 'op' to 'local_'
  void markResult(AbstractOperand* op);
  void sortSlots();

  OperandIndex operandIndex(AbstractOperand* op) const;

  // Input Operand  to offset in operands array.
  folly::F14FastMap<AbstractOperand*, int32_t> input_;

  // Local/output Operand offset in operands array.
  folly::F14FastMap<AbstractOperand*, int32_t> local_;

  // Owns device side 'threadBlockProgram_'
  WaveBufferPtr deviceData_;

  // Device resident program.
  ThreadBlockProgram* program_;

  int32_t sharedMemorySize_{0};
  // Serializes 'prepared_'. Access on WaveStrea, is single threaded but sharing
  // Programs across WaveDrivers makes sense, so make the preallocated resource
  // thread safe.
  std::mutex mutex_;

  // a pool of ready to run executables.
  std::vector<std::unique_ptr<Executable>> prepared_;
};

using ProgramPtr = std::shared_ptr<Program>;

struct LaunchControl;

/// Represents consecutive data dependent kernel launches.
class WaveStream {
 public:
  WaveStream(GpuArena& arena) : arena_(arena) {}

  ~WaveStream();

  // Binds operands of each program to inputs from pending programs and if
  // depending on more than one Wave, adds dependency via events. Each program
  // [i]is dimensioned to have  sizes[i] max intermediates/results.
  void startWave(
      folly::Range<Executable**> programs,
      folly::Range<int32_t*> sizes);

  GpuArena& arena() {
    return arena_;
  }

  void getOutput(
      folly::Range<const OperandId*> operands,
      WaveVectorPtr* waveVectors);

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
  /// WaveOperator. 'inputRows' is the logical number of input rows,
  /// not all TBs are necessarily full. 'exes' are the programs
  /// launched together, e.g. different exprs on different
  /// columns. 'blocks{PerExe' is the number of TBs running each exe. 'stream'
  /// enqueus the data transfer.
  LaunchControl* prepareProgramLaunch(
      int32_t key,
      int32_t inputRows,
      folly::Range<Executable**> exes,
      int32_t blocksPerExe,
      bool initstatus,
      Stream* stream);

  const std::vector<std::unique_ptr<LaunchControl>>& launchControls(
      int32_t key) {
    return launchControl_[key];
  }

 private:
  Event* newEvent();

  static std::unique_ptr<Event> eventFromReserve();
  static void releaseEvent(std::unique_ptr<Event>&& event);

  // Preallocated Streams and Events.
  static std::mutex reserveMutex_;
  static std::vector<std::unique_ptr<Event>> eventsForReuse_;
  static std::vector<std::unique_ptr<Stream>> streamsForReuse_;
  static bool exitInited_;

  static void clearReusable();

  GpuArena& arena_;
  folly::F14FastMap<OperandId, Executable*> operandToExecutable_;
  std::vector<std::unique_ptr<Executable>> executables_;

  // Currently active streams, each at the position given by its
  // stream->userData().
  std::vector<std::unique_ptr<Stream>> streams_;
  // The most recent event recorded on the pairwise corresponding element of
  // 'streams_'.
  std::vector<Event*> lastEvent_;

  // all events recorded on any stream. Events, once seen realized, are moved
  // back to reserve from here.
  folly::F14FastSet<Event*> allEvents_;

  // invocation record with return status blocks for programs. Used for getting
  // errors and filter cardinalities on return of  specific exes.
  folly::F14FastMap<int32_t, std::vector<std::unique_ptr<LaunchControl>>>
      launchControl_;

  folly::F14FastMap<int32_t, WaveBufferPtr> extraData_;
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
  int32_t key;

  int32_t inputRows;

  /// The first thread block with the program.
  int32_t* blockBase;
  // The ordinal of the program. All blocks with the same program have the same
  // number here.
  int32_t* programIdx;

  // The TB program for each exe.
  ThreadBlockProgram** programs;

  // For each exe, the start of the array of Operand*. Instructions reference
  // operands via offset in this array.//
  Operand*** operands;

  // the status return block for each TB.
  BlockStatus* status;
  int32_t sharedMemorySize{0};

  // Storage for all the above in a contiguous unified memory piece.
  WaveBufferPtr deviceData;
};

} // namespace facebook::velox::wave
