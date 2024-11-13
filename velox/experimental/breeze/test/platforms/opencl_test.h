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

/*
 * Copyright (c) 2024 by Rivos Inc.
 * Licensed under the Apache License, Version 2.0, see LICENSE for details.
 * SPDX-License-Identifier: Apache-2.0
 */

#include <type_traits>
#include <vector>

#define CL_TARGET_OPENCL_VERSION 300
#include <CL/cl.h>

#define CL_CALL(...)                                                         \
  do {                                                                       \
    cl_int e = __VA_ARGS__;                                                  \
    if (e != CL_SUCCESS) {                                                   \
      fprintf(stderr, "OpenCL failure: %s:%d: %d\n", __FILE__, __LINE__, e); \
      __builtin_trap();                                                      \
    }                                                                        \
  } while (0)

// 16 MiB
enum { MAX_BINARY_SIZE = 0x1000000 };

struct OpenCLTest {
  OpenCLTest() {
    // Load binary from the shader library.
    FILE* fp = NULL;
    fp = fopen(SHADER_LIB, "r");
    if (!fp) {
      fprintf(stderr, "failed to load shader library: " SHADER_LIB "\n");
      exit(1);
    }
    auto binary_data = malloc(MAX_BINARY_SIZE);
    size_t binary_size = fread(binary_data, 1, MAX_BINARY_SIZE, fp);
    fclose(fp);
    if (binary_size == MAX_BINARY_SIZE) {
      fprintf(stderr, "shader library is too large, MAX_BINARY_SIZE=%d\n",
              MAX_BINARY_SIZE);
      exit(1);
    }
    // Get platform and device.
    cl_platform_id platform_id = nullptr;
    cl_uint num_platforms = 0;
    CL_CALL(clGetPlatformIDs(1, &platform_id, &num_platforms));
    cl_device_id device_id = nullptr;
    cl_uint num_devices = 0;
    CL_CALL(clGetDeviceIDs(platform_id, CL_DEVICE_TYPE_DEFAULT, 1, &device_id,
                           &num_devices));
    // Create context and command queue.
    cl_int ret = -1;
    context = clCreateContext(nullptr, 1, &device_id, nullptr, nullptr, &ret);
    CL_CALL(ret);
    command_queue =
        clCreateCommandQueueWithProperties(context, device_id, nullptr, &ret);
    CL_CALL(ret);
    // Create a program from the loaded binary.
    cl_int binary_status = 0;
    program = clCreateProgramWithBinary(context, 1, &device_id, &binary_size,
                                        (const unsigned char**)&binary_data,
                                        &binary_status, &ret);
    CL_CALL(ret);
    CL_CALL(clBuildProgram(program, 1, &device_id, nullptr, nullptr, nullptr));
  }
  ~OpenCLTest() {
    CL_CALL(clReleaseProgram(program));
    CL_CALL(clReleaseCommandQueue(command_queue));
    CL_CALL(clReleaseContext(context));
  }
  template <int BLOCK_THREADS>
  void dispatchKernel(cl_kernel kernel, int numBlocks) {
    size_t global_item_size = BLOCK_THREADS * numBlocks;
    size_t local_item_size = BLOCK_THREADS;
    CL_CALL(clEnqueueNDRangeKernel(command_queue, kernel, 1, nullptr,
                                   &global_item_size, &local_item_size, 0,
                                   nullptr, nullptr));
    CL_CALL(clFinish(command_queue));
  }
  cl_context context = nullptr;
  cl_command_queue command_queue = nullptr;
  cl_program program = nullptr;
};

template <typename T>
struct ScopedDeviceBuffer {
  ScopedDeviceBuffer(OpenCLTest* test, const std::vector<T>& host)
      : test(test), size(host.size()) {
    cl_int ret = -1;
    mem = clCreateBuffer(test->context, CL_MEM_READ_ONLY, size * sizeof(T),
                         nullptr, &ret);
    CL_CALL(ret);
    CL_CALL(clEnqueueWriteBuffer(test->command_queue, mem, CL_TRUE, 0,
                                 size * sizeof(T), host.data(), 0, nullptr,
                                 nullptr));
  }
  ScopedDeviceBuffer(OpenCLTest* test, std::vector<T>& host)
      : test(test), mutable_host(host.data()), size(host.size()) {
    cl_int ret = -1;
    mem = clCreateBuffer(test->context, CL_MEM_READ_WRITE, size * sizeof(T),
                         nullptr, &ret);
    CL_CALL(ret);
    CL_CALL(clEnqueueWriteBuffer(test->command_queue, mem, CL_TRUE, 0,
                                 size * sizeof(T), host.data(), 0, nullptr,
                                 nullptr));
  }
  ScopedDeviceBuffer(OpenCLTest* test, const T* host) : test(test), size(1) {
    cl_int ret = -1;
    mem = clCreateBuffer(test->context, CL_MEM_READ_ONLY, size * sizeof(T),
                         nullptr, &ret);
    CL_CALL(ret);
    CL_CALL(clEnqueueWriteBuffer(test->command_queue, mem, CL_TRUE, 0,
                                 size * sizeof(T), host, 0, nullptr, nullptr));
  }

  // Move-only
  ScopedDeviceBuffer(const ScopedDeviceBuffer<T>&) = delete;
  ScopedDeviceBuffer<T>& operator=(const ScopedDeviceBuffer<T>&) = delete;
  ScopedDeviceBuffer(ScopedDeviceBuffer<T>&& other) {
    std::swap(test, other.test);
    std::swap(mutable_host, other.mutable_host);
    std::swap(size, other.size);
    std::swap(mem, other.mem);
  }

  ~ScopedDeviceBuffer() {
    if (mutable_host) {
      CL_CALL(clEnqueueReadBuffer(test->command_queue, mem, CL_TRUE, 0,
                                  size * sizeof(T), mutable_host, 0, nullptr,
                                  nullptr));
    }
    if (mem) {
      CL_CALL(clReleaseMemObject(mem));
    }
  }
  OpenCLTest* test = nullptr;
  T* mutable_host = nullptr;
  size_t size = 0;
  cl_mem mem = nullptr;
};

struct ScopedKernel {
  ScopedKernel(OpenCLTest* test, const char* kernelName) : test(test) {
    cl_int ret = -1;
    kernel = clCreateKernel(test->program, kernelName, &ret);
    CL_CALL(ret);
  }
  ~ScopedKernel() { CL_CALL(clReleaseKernel(kernel)); }
  void pushArg(const cl_mem mem) {
    int index = num_args++;
    CL_CALL(clSetKernelArg(kernel, index, sizeof(cl_mem), &mem));
  }
  OpenCLTest* test = nullptr;
  cl_kernel kernel = nullptr;
  int num_args = 0;
};

template <typename T>
struct is_std_vector : std::false_type {};
template <typename T>
struct is_std_vector<std::vector<T>> : std::true_type {};

template <typename T, typename BaseType = std::enable_if_t<
                          is_std_vector<std::remove_const_t<T>>::value,
                          typename T::value_type>>
ScopedDeviceBuffer<BaseType> MakeBuffer(OpenCLTest* test, T& arg) {
  return ScopedDeviceBuffer<BaseType>(test, arg);
}

template <typename T,
          typename = std::enable_if_t<std::is_arithmetic_v<T>, void>>
ScopedDeviceBuffer<T> MakeBuffer(OpenCLTest* test, T& arg) {
  return ScopedDeviceBuffer<T>(test, &arg);
}

template <int BLOCK_THREADS, typename... Args>
void OpenCLTestDispatch(int numBlocks, OpenCLTest* test, const char* kernelName,
                        Args&&... args) {
  // TODO: Using clGetKernelInfo and clGetKernelArgInfo one could validate that
  // the provided arguments match the kernel's expectations.

  // To be conservative only accept vectors and arithmetic types for now.
  static_assert(
      ((is_std_vector<
            std::remove_const_t<std::remove_reference_t<Args>>>::value ||
        std::is_arithmetic_v<Args>) &&
       ...));

  [&](auto... deviceBuffers) {
    ScopedKernel kernel(test, kernelName);
    (kernel.pushArg(deviceBuffers.mem), ...);
    test->dispatchKernel<BLOCK_THREADS>(kernel.kernel, numBlocks);
  }(MakeBuffer(test, args)...);
}
