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

#include "velox/experimental/wave/exec/AggregationInstructions.h"

#include <cub/cub.cuh> // @manual
#include "velox/experimental/wave/common/IdMap.cuh"
#include "velox/experimental/wave/exec/AggregateFunctionRegistry.h"
#include "velox/experimental/wave/exec/BuiltInAggregateFunctions.cuh"
#include "velox/experimental/wave/exec/WaveCore.cuh"

#define VELOX_WAVE_RETURN_NOT_OK(_expr)            \
  if (auto _ec = (_expr); _ec != ErrorCode::kOk) { \
    return _ec;                                    \
  }

#ifdef NDEBUG
#define LOG_TYPE_DISPATCH_ERROR(_kind)
#else
#define LOG_TYPE_DISPATCH_ERROR(_kind) \
  printf("%s:%d: Unsupported type %d\n", __FILE__, __LINE__, _kind)
#endif

#define KEY_TYPE_DISPATCH(_func, _kindExpr, ...) \
  [&]() {                                        \
    auto _kind = (_kindExpr);                    \
    switch (_kind) {                             \
      case PhysicalType::kInt32:                 \
        return _func<int32_t>(__VA_ARGS__);      \
      case PhysicalType::kInt64:                 \
        return _func<int64_t>(__VA_ARGS__);      \
      case PhysicalType::kString:                \
        return _func<StringView>(__VA_ARGS__);   \
      default:                                   \
        LOG_TYPE_DISPATCH_ERROR(_kind);          \
        return ErrorCode::kError;                \
    };                                           \
  }()

#define VALUE_TYPE_DISPATCH(_func, _kindExpr, ...) \
  [&]() {                                          \
    auto _kind = (_kindExpr);                      \
    switch (_kind) {                               \
      case PhysicalType::kInt8:                    \
        return _func<int8_t>(__VA_ARGS__);         \
      case PhysicalType::kInt16:                   \
        return _func<int16_t>(__VA_ARGS__);        \
      case PhysicalType::kInt32:                   \
        return _func<int32_t>(__VA_ARGS__);        \
      case PhysicalType::kInt64:                   \
        return _func<int64_t>(__VA_ARGS__);        \
      case PhysicalType::kFloat32:                 \
        return _func<float>(__VA_ARGS__);          \
      case PhysicalType::kFloat64:                 \
        return _func<double>(__VA_ARGS__);         \
      case PhysicalType::kString:                  \
        return _func<StringView>(__VA_ARGS__);     \
      default:                                     \
        LOG_TYPE_DISPATCH_ERROR(_kind);            \
        return ErrorCode::kError;                  \
    };                                             \
  }()

namespace facebook::velox::wave::aggregation {

namespace {

constexpr int kNormalizationRadix = 4;

struct BlockInfo {
  int base;
  char* shared;
};

template <typename T>
__device__ ErrorCode
normalize(BlockInfo* block, void* idMap, Operand* key, int32_t& result) {
  auto* typedIdMap = reinterpret_cast<IdMap<T>*>(idMap);
  auto id = typedIdMap->makeId(value<T>(key, block->base, block->shared));
  if (id == -1) {
    return ErrorCode::kInsufficientMemory;
  }
  assert(typedIdMap->cardinality() <= kNormalizationRadix);
  result = kNormalizationRadix * result + id - 1;
  return ErrorCode::kOk;
}

template <typename T>
__device__ ErrorCode setGroupKey(
    BlockInfo* block,
    NormalizeKeys* normalizeKeys,
    int keyIndex,
    int groupIndex) {
  auto* container = normalizeKeys->container;
  *reinterpret_cast<T*>(container->groups[groupIndex].keys[keyIndex]) =
      value<T>(&normalizeKeys->inputs[keyIndex], block->base, block->shared);
  return ErrorCode::kOk;
}

__device__ ErrorCode run(BlockInfo* block, NormalizeKeys* normalizeKeys) {
  auto size = normalizeKeys->inputs[0].size;
  assert(normalizeKeys->result->size == size);
  if (threadIdx.x + block->base >= size) {
    return ErrorCode::kOk;
  }
  int32_t result = 0;
  auto* container = normalizeKeys->container;
  for (int i = 0; i < container->numKeys; ++i) {
    VELOX_WAVE_RETURN_NOT_OK(KEY_TYPE_DISPATCH(
        normalize,
        container->keyTypes[i].kind,
        block,
        container->idMaps[i],
        &normalizeKeys->inputs[i],
        result));
  }
  assert(result < container->numGroups);
  if (!atomicExch(&container->groups[result].initialized, 1)) {
    atomicAdd(&container->actualNumGroups, 1);
    for (int i = 0; i < container->numKeys; ++i) {
      VELOX_WAVE_RETURN_NOT_OK(KEY_TYPE_DISPATCH(
          setGroupKey,
          container->keyTypes[i].kind,
          block,
          normalizeKeys,
          i,
          result));
    }
  }
  flatResult<int32_t>(normalizeKeys->result, block->base) = result;
  return ErrorCode::kOk;
}

// Only one block should be writing to the same accumulator.
__device__ ErrorCode run(BlockInfo*, Aggregate* aggregate) {
  assert(aggregate->normalizedKey);
  assert(aggregate->container->useThreadLocalAccumulator);
  auto* function = aggregate->function;
  for (int i = threadIdx.x; i < aggregate->normalizedKey->size;
       i += blockDim.x) {
    auto key = value<int32_t>(aggregate->normalizedKey, i);
    auto& group = aggregate->container->groups[key];
    void* accumulator = (char*)group.accumulators[aggregate->accumulatorIndex] +
        threadIdx.x * function->accumulatorSize;
    // TODO: Try inline small common aggregate functions.
    VELOX_WAVE_RETURN_NOT_OK(function->addRawInput(
        aggregate->numInputs, aggregate->inputs, i, accumulator));
  }
  return ErrorCode::kOk;
}

template <typename T>
__device__ ErrorCode extractKey(Operand* result, int i, void* key) {
  reinterpret_cast<T*>(result->base)[i] = *reinterpret_cast<const T*>(key);
  return ErrorCode::kOk;
}

__device__ ErrorCode run(BlockInfo* block, ExtractKeys* extractKeys) {
  using Scan = cub::BlockScan<int, kBlockSize>;
  auto* tmp = reinterpret_cast<Scan::TempStorage*>(block->shared);
  auto* container = extractKeys->container;
  for (int i = threadIdx.x; i / kBlockSize * blockDim.x < container->numGroups;
       i += blockDim.x) {
    int outIndex =
        i < container->numGroups ? container->groups[i].initialized : 0;
    Scan(*tmp).ExclusiveSum(outIndex, outIndex);
    __syncthreads();
    if (i >= container->numGroups) {
      break;
    }
    if (container->groups[i].initialized) {
      KEY_TYPE_DISPATCH(
          extractKey,
          container->keyTypes[extractKeys->keyIndex].kind,
          extractKeys->result,
          outIndex,
          container->groups[i].keys[extractKeys->keyIndex]);
    }
  }
  return ErrorCode::kOk;
}

__device__ ErrorCode run(BlockInfo* block, ExtractValues* extractValues) {
  using Reduce = cub::BlockReduce<void*, kBlockSize>;
  auto* tmp = reinterpret_cast<Reduce::TempStorage*>(block->shared);
  auto* container = extractValues->container;
  assert(container->useThreadLocalAccumulator);
  auto* function = extractValues->function;
  for (int i = 0, outIndex = 0; i < container->numGroups; ++i) {
    auto& group = container->groups[i];
    if (!group.initialized) {
      continue;
    }
    void* accumulator =
        (char*)group.accumulators[extractValues->accumulatorIndex] +
        threadIdx.x * function->accumulatorSize;
    __syncthreads();
    accumulator =
        Reduce(*tmp).Reduce(accumulator, [function](void* a, void* b) {
          return function->mergeAccumulators(a, b);
        });
    __syncthreads();
    auto* ec = reinterpret_cast<ErrorCode*>(block->shared);
    if (threadIdx.x == 0) {
      *ec =
          function->extractValues(accumulator, extractValues->result, outIndex);
    }
    __syncthreads();
    VELOX_WAVE_RETURN_NOT_OK(*ec);
    ++outIndex;
  }
  return ErrorCode::kOk;
}

__global__ void runPrograms(
    ThreadBlockProgram* programs,
    int32_t* baseIndices,
    BlockStatus* blockStatusArray) {
  extern __shared__ __align__(64) char shared[];
  int baseIndex = baseIndices ? baseIndices[blockIdx.x] : 0;
  BlockInfo block = {
      .base = (int)(blockDim.x * (blockIdx.x - baseIndex)),
      .shared = shared,
  };
  auto& status = blockStatusArray[blockIdx.x];
  auto& program = programs[blockIdx.x];
  assert(status.errors[threadIdx.x] == ErrorCode::kOk);
  for (auto i = 0; i < program.numInstructions; ++i) {
    if (status.errors[threadIdx.x] != ErrorCode::kOk) {
      break;
    }
    auto& instruction = program.instructions[i];
    switch (instruction.opCode) {
      case OpCode::kNormalizeKeys:
        status.errors[threadIdx.x] = run(&block, &instruction._.normalizeKeys);
        break;
      case OpCode::kAggregate:
        status.errors[threadIdx.x] = run(&block, &instruction._.aggregate);
        break;
      case OpCode::kExtractKeys:
        status.errors[threadIdx.x] = run(&block, &instruction._.extractKeys);
        break;
      case OpCode::kExtractValues:
        status.errors[threadIdx.x] = run(&block, &instruction._.extractValues);
        break;
      default:
#ifndef NDEBUG
        printf(
            "%s:%d: Unsupported OpCode %d\n",
            __FILE__,
            __LINE__,
            instruction.opCode);
#endif
        status.errors[threadIdx.x] = ErrorCode::kError;
    }
  }
  assert(status.errors[threadIdx.x] == ErrorCode::kOk);
}

} // namespace

AggregateFunctionRegistry::AggregateFunctionRegistry(GpuAllocator* allocator)
    : allocator_(allocator) {}

// The definitions of concrete functions must be in one compilation unit, same
// as where they are used in kernel.
void AggregateFunctionRegistry::addAllBuiltInFunctions(Stream& stream) {
  // TODO: Parallelize the kernel calls.
  Entry entry;

  entry.accept = [](auto&) { return true; };
  entry.function = allocator_->allocate<AggregateFunction>();
  createFunction<Count>
      <<<1, 1, 0, stream.stream()->stream>>>(entry.function.get());
  CUDA_CHECK(cudaGetLastError());
  stream.wait();
  entries_["count"].push_back(std::move(entry));

  entry.accept = [](auto& argTypes) {
    return argTypes.size() == 1 && argTypes[0].kind == PhysicalType::kInt32;
  };
  entry.function = allocator_->allocate<AggregateFunction>();
  createFunction<Sum<int32_t, int64_t>>
      <<<1, 1, 0, stream.stream()->stream>>>(entry.function.get());
  CUDA_CHECK(cudaGetLastError());
  stream.wait();
  entries_["sum"].push_back(std::move(entry));

  entry.accept = [](auto& argTypes) {
    return argTypes.size() == 1 && argTypes[0].kind == PhysicalType::kInt64;
  };
  entry.function = allocator_->allocate<AggregateFunction>();
  createFunction<Sum<int64_t, int64_t>>
      <<<1, 1, 0, stream.stream()->stream>>>(entry.function.get());
  CUDA_CHECK(cudaGetLastError());
  stream.wait();
  entries_["sum"].push_back(std::move(entry));

  entry.accept = [](auto& argTypes) {
    return argTypes.size() == 1 && argTypes[0].kind == PhysicalType::kFloat64;
  };
  entry.function = allocator_->allocate<AggregateFunction>();
  createFunction<Sum<double, double>>
      <<<1, 1, 0, stream.stream()->stream>>>(entry.function.get());
  CUDA_CHECK(cudaGetLastError());
  stream.wait();
  entries_["sum"].push_back(std::move(entry));

  entry.accept = [](auto& argTypes) {
    return argTypes.size() == 1 && argTypes[0].kind == PhysicalType::kInt64;
  };
  entry.function = allocator_->allocate<AggregateFunction>();
  createFunction<Avg<int64_t>>
      <<<1, 1, 0, stream.stream()->stream>>>(entry.function.get());
  CUDA_CHECK(cudaGetLastError());
  stream.wait();
  entries_["avg"].push_back(std::move(entry));

  entry.accept = [](auto& argTypes) {
    return argTypes.size() == 1 && argTypes[0].kind == PhysicalType::kFloat64;
  };
  entry.function = allocator_->allocate<AggregateFunction>();
  createFunction<Avg<double>>
      <<<1, 1, 0, stream.stream()->stream>>>(entry.function.get());
  CUDA_CHECK(cudaGetLastError());
  stream.wait();
  entries_["avg"].push_back(std::move(entry));
}

AggregateFunction* AggregateFunctionRegistry::getFunction(
    const std::string& name,
    const Types& argTypes) const {
  auto it = entries_.find(name);
  if (it == entries_.end()) {
    return nullptr;
  }
  for (int i = it->second.size() - 1; i >= 0; --i) {
    if (it->second[i].accept(argTypes)) {
      return it->second[i].function.get();
    }
  }
  return nullptr;
}

int ExtractKeys::sharedSize() {
  return sizeof(cub::BlockScan<int, kBlockSize>::TempStorage);
}

int ExtractValues::sharedSize() {
  return sizeof(cub::BlockReduce<void*, kBlockSize>::TempStorage);
}

void call(
    Stream& stream,
    int numBlocks,
    ThreadBlockProgram* programs,
    int32_t* baseIndices,
    BlockStatus* status,
    int sharedSize) {
  runPrograms<<<numBlocks, kBlockSize, sharedSize, stream.stream()->stream>>>(
      programs, baseIndices, status);
  CUDA_CHECK(cudaGetLastError());
}

} // namespace facebook::velox::wave::aggregation
