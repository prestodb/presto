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

#include <folly/init/Init.h>
#include <gflags/gflags.h>
#include <cuda/atomic> // @manual
#include "velox/experimental/gpu/Common.h"

DEFINE_int32(grid_size, 1024, "");
DEFINE_int32(node_count, 4000, "");
DEFINE_int32(host_threads, 10, "");
DEFINE_int32(running_time_seconds, 5, "");
DEFINE_int32(backoff_initial_nanoseconds, 8, "");

struct Node {
  cuda::atomic<int32_t> dependencies;
};

struct States {
  int backoffInitialNanoseconds;
  cuda::atomic<bool> running;
  cuda::atomic<int64_t> start;
  cuda::atomic<int64_t> finish;
};

__device__ int findReadyNode(Node* nodes, int size, States& states) {
  int backoff = states.backoffInitialNanoseconds;
  while (states.running) {
    for (int i = 0; i < size; ++i) {
      int ready = 0;
      if (nodes[i].dependencies.compare_exchange_strong(ready, -1)) {
        return i;
      }
    }
    __nanosleep(backoff);
    backoff *= 2;
  }
  return -1;
}

int findAvailableNode(Node* nodes, int size, States& states) {
  int backoff = states.backoffInitialNanoseconds;
  while (states.running) {
    for (int i = 0; i < size; ++i) {
      int avail = -1;
      if (nodes[i].dependencies.compare_exchange_strong(avail, 0)) {
        ++states.start;
        return i;
      }
    }
    std::this_thread::sleep_for(std::chrono::nanoseconds(backoff));
    backoff *= 2;
  }
  return -1;
}

__global__ void pickUpWork(Node* nodes, int size, States& states) {
  __shared__ int nodeIndex;
  // if (threadIdx.x == 0) {
  //   printf("Starting running block %d\n", blockIdx.x);
  // }
  while (states.running) {
    if (threadIdx.x == 0) {
      nodeIndex = findReadyNode(nodes, size, states);
    }
    __syncthreads();
    if (nodeIndex == -1) {
      return;
    }
    // printf("Pick up work for node %d on block %d thread %d\n", nodeIndex,
    // blockIdx.x, threadIdx.x);
    __syncthreads();
    if (threadIdx.x == 0) {
      ++states.finish;
    }
  }
}

void schedule(Node* nodes, int size, States* states) {
  while (states->running) {
    findAvailableNode(nodes, size, *states);
  }
}

int main(int argc, char** argv) {
  using namespace facebook::velox::gpu;
  folly::Init init{&argc, &argv};
  Node* nodes;
  CUDA_CHECK_FATAL(cudaMallocManaged(&nodes, FLAGS_node_count * sizeof(Node)));
  for (int i = 0; i < FLAGS_node_count; ++i) {
    nodes[i].dependencies = -1;
  }
  States* states;
  CUDA_CHECK_FATAL(cudaMallocManaged(&states, sizeof(States)));
  states->backoffInitialNanoseconds = FLAGS_backoff_initial_nanoseconds;
  states->running = true;
  states->start = 0;
  states->finish = 0;
  pickUpWork<<<FLAGS_grid_size, 32>>>(nodes, FLAGS_node_count, *states);
  CUDA_CHECK_FATAL(cudaGetLastError());
  std::vector<std::thread> threads;
  for (int i = 0; i < FLAGS_host_threads; ++i) {
    threads.emplace_back(schedule, nodes, FLAGS_node_count, states);
  }
  std::chrono::seconds runningTime(FLAGS_running_time_seconds);
  std::this_thread::sleep_for(runningTime);
  states->running = false;
  for (auto& t : threads) {
    t.join();
  }
  CUDA_CHECK_FATAL(cudaDeviceSynchronize());
  printf(
      "Started: %ld, Finished: %ld\n",
      states->start.load(),
      states->finish.load());
  printf(
      "%.2f ns per node\n",
      1.0 * std::chrono::nanoseconds(runningTime).count() /
          states->finish.load());
  CUDA_CHECK_LOG(cudaFree(states));
  CUDA_CHECK_LOG(cudaFree(nodes));
  return 0;
}
