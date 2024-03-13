#include "velox/experimental/wave/common/Block.cuh"
#include "velox/experimental/wave/common/CudaUtil.cuh"
#include "velox/experimental/wave/common/tests/BlockTest.h"

namespace facebook::velox::wave {

using ScanAlgorithm = cub::BlockScan<int, 256, cub::BLOCK_SCAN_RAKING>;

__global__ void boolToIndices(
    uint8_t** bools,
    int32_t** indices,
    int32_t* sizes,
    int64_t* times) {
  extern __shared__ __align__(alignof(ScanAlgorithm::TempStorage)) char smem[];
  int32_t idx = blockIdx.x;
  // Start cycle timer
  clock_t start = clock();
  uint8_t* blockBools = bools[idx];
  boolBlockToIndices<256>(
      [&]() { return blockBools[threadIdx.x]; },
      idx * 256,
      indices[idx],
      smem,
      sizes[idx]);
  clock_t stop = clock();
  if (threadIdx.x == 0) {
    times[idx] = (start > stop) ? start - stop : stop - start;
  }
}

void BlockTestStream::testBoolToIndices(
    int32_t numBlocks,
    uint8_t** flags,
    int32_t** indices,
    int32_t* sizes,
    int64_t* times) {
  CUDA_CHECK(cudaGetLastError());
  auto tempBytes = sizeof(typename ScanAlgorithm::TempStorage);
  boolToIndices<<<numBlocks, 256, tempBytes, stream_->stream>>>(
      flags, indices, sizes, times);
  CUDA_CHECK(cudaGetLastError());
}

__global__ void sum64(int64_t* numbers, int64_t* results) {
  extern __shared__ __align__(
      alignof(cub::BlockReduce<int64_t, 256>::TempStorage)) char smem[];
  int32_t idx = blockIdx.x;
  blockSum<256>(
      [&]() { return numbers[idx * 256 + threadIdx.x]; }, smem, results);
}

void BlockTestStream::testSum64(
    int32_t numBlocks,
    int64_t* numbers,
    int64_t* results) {
  auto tempBytes = sizeof(typename cub::BlockReduce<int64_t, 256>::TempStorage);
  sum64<<<numBlocks, 256, tempBytes, stream_->stream>>>(numbers, results);
  CUDA_CHECK(cudaGetLastError());
}

} // namespace facebook::velox::wave
