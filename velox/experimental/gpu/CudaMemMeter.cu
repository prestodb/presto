/* Copyright (c) 1993-2015, NVIDIA CORPORATION. All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *  * Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *  * Neither the name of NVIDIA CORPORATION nor the names of its
 *    contributors may be used to endorse or promote products derived
 *    from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS ``AS IS'' AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 * PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL THE COPYRIGHT OWNER OR
 * CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY
 * OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#include <assert.h>
#include <stdio.h>
#include <chrono>
#include <thread>
#include <vector>

// Convenience function for checking CUDA runtime API results
// can be wrapped around any runtime API call. No-op in release builds.
inline cudaError_t checkCuda(cudaError_t result) {
  if (result != cudaSuccess) {
    fprintf(stderr, "CUDA Runtime Error: %s\n", cudaGetErrorString(result));
    assert(result == cudaSuccess);
  }
  return result;
}

__global__ void kernel(float* a, int offset) {
  int i = offset + threadIdx.x + blockIdx.x * blockDim.x;
  a[i] += i;
}

int64_t millis(std::chrono::steady_clock::time_point start) {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::steady_clock::now() - start)
      .count();
}

// Arguments: [-1] n-repeats n-streams kbytesperstream dev1 dev2 ...
int main(int argc, char** argv) {
  int nRepeats = 1;
  int nStreams = 4;
  int n256floats = 4096;
  if (argc < 4) {
    printf(
        "Usage CudaMemMeter [-1] numRepeats  numStreams KB [dev-1 dev-2 ...]\n"
        " Copies KB bytes to device and back numRepeats times. If doing "
        "this with multiple streams, uses numstreams streams.\n"
        "Runs the above for each of the devices listed after KB:\n"
        "CudaMemMeter 100 10e 200 0 0 1 1 will do 100 repeats of a 200K transfer to and "
        "from \n"
        "device. Each of these transfers will be divided into 10 parallel\n"
        "streams. This will be run twice on two host threads going to \n"
        "device 0 and two host threads going to device 1.");
    return 1;
  }
  int32_t firstArg = argv[1][0] == '-' && argv[1][1] == '1' ? 1 : 0;
  bool oneWay = firstArg == 1;
  float multiplier = oneWay ? 1.0 : 2.0;
  if (argc >= firstArg + 4) {
    nRepeats = atoi(argv[firstArg + 1]);
    nStreams = atoi(argv[firstArg + 2]);
    n256floats = atoi(argv[firstArg + 3]);
  }
  const int blockSize = 256;
  const long n = n256floats * blockSize * nStreams;
  const long streamSize = n / nStreams;
  const long streamBytes = streamSize * sizeof(float);
  const long bytes = n * sizeof(float);

  int devId = 0;
  if (argc > firstArg + 4) {
    devId = atoi(argv[firstArg + 4]);
  }
  std::vector<int32_t> devices;
  devices.push_back(devId);
  printf("Devices: %d", devId);
  for (auto i = firstArg + 5; i < argc; ++i) {
    devices.push_back(atoi(argv[i]));
    printf(", %d", devices.back());
  }
  printf("\n");
  cudaDeviceProp prop;
  checkCuda(cudaGetDeviceProperties(&prop, devId));
  printf("Device : %s\n", prop.name);
  std::vector<std::thread> threads;
  auto start = std::chrono::steady_clock::now();
  for (auto dev : devices) {
    threads.push_back(std::thread([dev,
                                   n,
                                   start,
                                   streamSize,
                                   blockSize,
                                   bytes,
                                   streamBytes,
                                   nRepeats,
                                   nStreams,
                                   oneWay,
                                   multiplier]() {
      checkCuda(cudaSetDevice(dev));
      // allocate pinned host memory and device memory
      float *a, *d_a;
      printf("%ldKB\n", bytes / 1024);
      checkCuda(cudaMallocHost((void**)&a, bytes)); // host pinned
      checkCuda(cudaMalloc((void**)&d_a, bytes)); // device

      float ms; // elapsed time in milliseconds
      float singleMs = 0;
      float async1Ms = 0;
      float async2Ms = 0;

      // create events and streams
      cudaEvent_t startEvent, stopEvent, dummyEvent;
      cudaStream_t* stream =
          (cudaStream_t*)malloc(nStreams * sizeof(cudaStream_t));
      checkCuda(cudaEventCreate(&startEvent));
      checkCuda(cudaEventCreate(&stopEvent));
      checkCuda(cudaEventCreate(&dummyEvent));
      for (int i = 0; i < nStreams; ++i) {
        checkCuda(cudaStreamCreate(&stream[i]));
      }
      // baseline case - sequential transfer and execute
      memset(a, 0, bytes);
      for (int repeat = 0; repeat < nRepeats; ++repeat) {
        checkCuda(cudaEventRecord(startEvent, 0));
        checkCuda(cudaMemcpy(d_a, a, bytes, cudaMemcpyHostToDevice));
        kernel<<<n / blockSize, blockSize>>>(d_a, 0);
        checkCuda(cudaGetLastError());
        if (!oneWay) {
          checkCuda(cudaMemcpy(a, d_a, bytes, cudaMemcpyDeviceToHost));
        }
        checkCuda(cudaEventRecord(stopEvent, 0));
        checkCuda(cudaEventSynchronize(stopEvent));
        checkCuda(cudaEventElapsedTime(&ms, startEvent, stopEvent));
        singleMs += ms;
      }
      float volume = bytes * nRepeats;
      printf(
          "%d at %ld: Time for sequential transfer and execute (ms): %f %f GB/s\n",
          dev,
          millis(start),
          singleMs,
          multiplier * (volume / (1 << 30)) / (singleMs / 1000));

      // asynchronous version 1: loop over {copy, kernel, copy}
      memset(a, 0, bytes);
      for (int repeat = 0; repeat < nRepeats; ++repeat) {
        checkCuda(cudaEventRecord(startEvent, 0));
        for (int i = 0; i < nStreams; ++i) {
          int offset = i * streamSize;
          checkCuda(cudaMemcpyAsync(
              &d_a[offset],
              &a[offset],
              streamBytes,
              cudaMemcpyHostToDevice,
              stream[i]));
          kernel<<<streamSize / blockSize, blockSize, 0, stream[i]>>>(
              d_a, offset);
          checkCuda(cudaGetLastError());
          if (!oneWay) {
            checkCuda(cudaMemcpyAsync(
                &a[offset],
                &d_a[offset],
                streamBytes,
                cudaMemcpyDeviceToHost,
                stream[i]));
          }
        }
        checkCuda(cudaEventRecord(stopEvent, 0));
        checkCuda(cudaEventSynchronize(stopEvent));
        checkCuda(cudaEventElapsedTime(&ms, startEvent, stopEvent));
        async1Ms += ms;
      }
      volume = streamBytes * nRepeats * nStreams;
      printf(
          "%d at %ld: Time for asynchronous V1 transfer and execute (ms): %f %f GB/s\n",
          dev,
          millis(start),
          async1Ms,
          multiplier * (volume / (1 << 30)) / (async1Ms / 1000));

      // asynchronous version 2:
      // loop over copy, loop over kernel, loop over copy
      memset(a, 0, bytes);
      for (int repeat = 0; repeat < nRepeats; ++repeat) {
        checkCuda(cudaEventRecord(startEvent, 0));
        for (int i = 0; i < nStreams; ++i) {
          int offset = i * streamSize;
          checkCuda(cudaMemcpyAsync(
              &d_a[offset],
              &a[offset],
              streamBytes,
              cudaMemcpyHostToDevice,
              stream[i]));
        }
        for (int i = 0; i < nStreams; ++i) {
          int offset = i * streamSize;
          kernel<<<streamSize / blockSize, blockSize, 0, stream[i]>>>(
              d_a, offset);
          checkCuda(cudaGetLastError());
        }
        for (int i = 0; i < nStreams; ++i) {
          int offset = i * streamSize;
          if (!oneWay) {
            checkCuda(cudaMemcpyAsync(
                &a[offset],
                &d_a[offset],
                streamBytes,
                cudaMemcpyDeviceToHost,
                stream[i]));
          }
        }
        checkCuda(cudaEventRecord(stopEvent, 0));
        checkCuda(cudaEventSynchronize(stopEvent));
        checkCuda(cudaEventElapsedTime(&ms, startEvent, stopEvent));
        async2Ms += ms;
      }
      volume = streamBytes * nRepeats * nStreams;
      printf(
          "%d at %ld: Time for asynchronous V2 transfer and execute (ms): %f %f GB/s\n",
          dev,
          millis(start),
          async2Ms,
          multiplier * (volume / (1 << 30)) / (async2Ms / 1000));

      // cleanup
      checkCuda(cudaEventDestroy(startEvent));
      checkCuda(cudaEventDestroy(stopEvent));
      checkCuda(cudaEventDestroy(dummyEvent));
      for (int i = 0; i < nStreams; ++i)
        checkCuda(cudaStreamDestroy(stream[i]));
      checkCuda(cudaFree(d_a));
      checkCuda(cudaFreeHost(a));
    }));
  }
  for (auto& thread : threads) {
    thread.join();
  }
  printf("At %ld: Completed %ld threads", millis(start), threads.size());
  return 0;
}
