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

#include <fmt/format.h>
#include <folly/Preprocessor.h>
#include <gflags/gflags.h>
#include <nvrtc.h>
#include "velox/experimental/wave/common/Cuda.h"
#include "velox/experimental/wave/common/CudaUtil.cuh"
#include "velox/experimental/wave/common/Exception.h"

#define JITIFY_PRINT_HEADER_PATHS 1

#include <filesystem>
#include "velox/experimental/wave/jit/Headers.h"
#include "velox/external/jitify/jitify.hpp"
DEFINE_bool(cuda_G, false, "Enable -G for NVRTC");

DEFINE_int32(
    cuda_O,
#ifndef NDEBUG
    0
#else
    3
#endif
    ,
    "-O level for NVRTC");

#ifndef VELOX_OSS_BUILD
#include "velox/facebook/NvrtcUtil.h"
#endif

namespace facebook::velox::wave {

void nvrtcCheck(nvrtcResult result) {
  if (result != NVRTC_SUCCESS) {
    waveError(nvrtcGetErrorString(result));
  }
}

class CompiledModuleImpl : public CompiledModule {
 public:
  CompiledModuleImpl(CUmodule module, std::vector<CUfunction> kernels)
      : module_(module), kernels_(std::move(kernels)) {}

  ~CompiledModuleImpl() {
    auto result = cuModuleUnload(module_);
    if (result != CUDA_SUCCESS) {
      LOG(ERROR) << "Error in unloading module " << result;
    }
  }

  void launch(
      int32_t kernelIdx,
      int32_t numBlocks,
      int32_t numThreads,
      int32_t shared,
      Stream* stream,
      void** args) override;

  KernelInfo info(int32_t kernelIdx) override;

 private:
  CUmodule module_;
  std::vector<CUfunction> kernels_;
};

namespace {

void addFlag(
    const char* flag,
    const char* value,
    int32_t length,
    std::vector<std::string>& data) {
  std::string str(flag);
  str.resize(str.size() + length);
  memcpy(str.data() + strlen(flag), value, length);
  data.push_back(std::move(str));
}

#ifdef VELOX_OSS_BUILD
void getDefaultNvrtcOptions(std::vector<std::string>& data) {
  constexpr const char* kUsrLocalCuda = "/usr/local/cuda/include";
  LOG(INFO) << "Using " << kUsrLocalCuda;
  addFlag("-I", kUsrLocalCuda, strlen(kUsrLocalCuda), data);
}
#endif

// Gets compiler options from the environment and appends  them  to 'data'.
void getNvrtcOptions(std::vector<std::string>& data) {
  const char* includes = getenv("WAVE_NVRTC_INCLUDE_PATH");
  if (includes && strlen(includes) > 0) {
    LOG(INFO) << "Found env NVRTC include path: " << includes;
    for (;;) {
      const char* end = strchr(includes, ':');
      if (!end) {
        addFlag("-I", includes, strlen(includes), data);
        break;
      }
      addFlag("-I", includes, end - includes, data);
      includes = end + 1;
    }
  } else {
    getDefaultNvrtcOptions(data);
  }
  const char* flags = getenv("WAVE_NVRTC_FLAGS");
  if (flags && strlen(flags)) {
    for (;;) {
      auto end = strchr(flags, ' ');
      if (!end) {
        addFlag("", flags, strlen(flags), data);
        break;
      }
      addFlag("", flags, end - flags, data);
      flags = end + 1;
    }
  }
}

// Contains header names and file contents as a std::string with a trailing 0.
std::vector<std::string> waveHeaderName;
std::vector<std::string> waveHeaderText;

//  data area of waveheader* as a null terminated string.
std::vector<const char*> waveHeaderNameString;
std::vector<const char*> waveHeaderTextString;
std::mutex initMutex;

std::vector<std::string> waveNvrtcFlags;
std::vector<const char*> waveNvrtcFlagsString;

bool readSystemHeaders(std::map<std::string, std::string>& headers) {
  std::ifstream in("/tmp/wavesystemheaders.txt");
  std::string path;
  while (std::getline(in, path)) {
    std::string size;
    getline(in, size);
    int32_t bytes = atoi(size.c_str());
    std::string text;
    text.resize(bytes);
    in.read(text.data(), bytes);
    headers[path] = text;
  }
  return !headers.empty();
}

void saveSystemHeaders(std::map<std::string, std::string>& map) {
  std::ofstream out(fmt::format("/tmp/h.{}", getpid()));
  for (auto& pair : map) {
    out << pair.first << std::endl
        << pair.second.size() << std::endl
        << pair.second;
  }
  out.close();
  system(fmt::format(" mv /tmp/h.{} /tmp/wavesystemheaders.txt", getpid())
             .c_str());
}

// Adds a trailing zero to make the string.data() a C char*.
void makeNTS(std::string& string) {
  string.resize(string.size() + 1);
  string.back() = 0;
}

void initializeWaveHeaders(
    const std::map<std::string, std::string>& headers,
    const std::string& except) {
  for (auto& pair : headers) {
    if (pair.first == except) {
      continue;
    }
    waveHeaderName.push_back(pair.first);
    makeNTS(waveHeaderName.back());
    waveHeaderText.push_back(pair.second);
    makeNTS(waveHeaderText.back());
  }
  for (auto i = 0; i < waveHeaderName.size(); ++i) {
    waveHeaderNameString.push_back(waveHeaderName[i].data());
    waveHeaderTextString.push_back(waveHeaderText[i].data());
  }
  std::vector<const char*> names;
  std::vector<const char*> text;
  getRegisteredHeaders(names, text);
  for (auto i = 0; i < names.size(); ++i) {
    waveHeaderNameString.push_back(names[i]);
    waveHeaderTextString.push_back(text[i]);
  }
}

// Uses Jitify to compile a sample program on initialization. This
// gathers the JIT-safe includes from Jitify and Cuda and Cub and
// Velox. This also decides flags architecture flags. The Wave kernel
// cache differs from Jitify in having multiple entry points and doing
// background compilation.
void ensureInit() {
  static std::atomic<bool> inited = false;
  if (inited) {
    return;
  }
  std::lock_guard<std::mutex> l(initMutex);

  if (inited) {
    return;
  }

  // Sample kernel that pulls in system headers used by Wave. Checks that key
  // headers are included and uses compile.
  const char* sampleText =
      "Sample\n"
      "#include <stdint.h>\n"
      "__global__ void \n"
      "sampleKernel(unsigned char** bools, int** mtx, int* sizes) { \n"
      "__shared__ int32_t f;\n"
      "if (threadIdx.x == 0) {\n"
      " f = 1;\n"
      " while (atomicCAS(*mtx, 1, 0) != 1) {\n"
      "  __nanosleep(1);\n"
      " }\n"
      " assert(f == 0);\n"
      " printf(\"pfaal\"); \n"
      "  atomicAdd(&f, 1);\n"
      "}\n"
      "} \n";

  waveNvrtcFlags.push_back("-std=c++17");
  if (FLAGS_cuda_O) {
    char str[10];
    sprintf(str, "-O%d", FLAGS_cuda_O);
    waveNvrtcFlags.push_back("-Xptxas");
    waveNvrtcFlags.push_back(std::string(str));
  }
  if (FLAGS_cuda_G) {
    waveNvrtcFlags.push_back("-G");
  }
  getNvrtcOptions(waveNvrtcFlags);
  auto device = currentDevice();
  bool hasArch = false;
  for (auto& flag : waveNvrtcFlags) {
    if (strstr(flag.c_str(), "-arch") != nullptr) {
      hasArch = true;
      break;
    }
  }
  if (!hasArch) {
    waveNvrtcFlags.push_back(fmt::format(
        "--gpu-architecture=compute_{}{}", device->major, device->minor));
  }
  waveNvrtcFlags.push_back("-DPLATFORM_CUDA");
  // FIXME: Use FOLLY_PP_STRINGIZE_MACRO when available.
#define PP_STRINGIZE_MACRO(x) FOLLY_PP_STRINGIZE(x)
  waveNvrtcFlags.push_back(
      "-DCUDA_PLATFORM_SPECIALIZATION_HEADER=" PP_STRINGIZE_MACRO(
          CUDA_PLATFORM_SPECIALIZATION_HEADER));
#undef PP_STRINGIZE_MACRO

  ::jitify::detail::detect_and_add_cuda_arch(waveNvrtcFlags);
  std::map<std::string, std::string> headers;
  if (!readSystemHeaders(headers)) {
    static jitify::JitCache kernel_cache;

    auto program = kernel_cache.program(sampleText, {}, waveNvrtcFlags);
    headers = program._impl->_config->sources;
    saveSystemHeaders(headers);
  }
  initializeWaveHeaders(headers, "sample");

  for (auto& str : waveNvrtcFlags) {
    makeNTS(str);
    waveNvrtcFlagsString.push_back(str.data());
  }
  LOG(INFO) << "NVRTC flags: ";
  for (auto i = 0; i < waveNvrtcFlagsString.size(); ++i) {
    LOG(INFO) << waveNvrtcFlagsString[i];
  }
  LOG(INFO) << "device=" << device->toString();
  inited = true;
}

} // namespace

// static
void CompiledModule::initialize() {
  ensureInit();
}

std::shared_ptr<CompiledModule> CompiledModule::create(const KernelSpec& spec) {
  ensureInit();
  const char** headers = waveHeaderTextString.data();
  const char** headerNames = waveHeaderNameString.data();
  int32_t numHeaders = waveHeaderNameString.size();
  std::vector<const char*> allHeaders;
  std::vector<const char*> allHeaderNames;
  // If spec has extra headers, add them after the system headers.
  if (spec.numHeaders > 0) {
    allHeaderNames = waveHeaderNameString;
    allHeaders = waveHeaderTextString;
    for (auto i = 0; i < spec.numHeaders; ++i) {
      allHeaders.push_back(spec.headers[i]);
      allHeaderNames.push_back(spec.headerNames[i]);
    }
    headers = allHeaders.data();
    headerNames = allHeaderNames.data();
    numHeaders += spec.numHeaders;
  }
  nvrtcProgram prog;
  nvrtcCreateProgram(
      &prog,
      spec.code.c_str(), // buffer
      spec.filePath.c_str(), // name
      numHeaders, // numHeaders
      headers, // headers
      headerNames); // includeNames
  for (auto& name : spec.entryPoints) {
    nvrtcCheck(nvrtcAddNameExpression(prog, name.c_str()));
  }

  auto compileResult = nvrtcCompileProgram(
      prog, // prog
      waveNvrtcFlagsString.size(), // numOptions
      waveNvrtcFlagsString.data()); // options

  size_t logSize;

  nvrtcGetProgramLogSize(prog, &logSize);
  std::string log;
  log.resize(logSize);
  nvrtcGetProgramLog(prog, log.data());

  if (compileResult != NVRTC_SUCCESS) {
    nvrtcDestroyProgram(&prog);
    waveError(std::string("Cuda compilation error: ") + log);
  }
  // Obtain PTX from the program.
  size_t ptxSize;
  nvrtcCheck(nvrtcGetPTXSize(prog, &ptxSize));
  std::string ptx;
  ptx.resize(ptxSize);
  nvrtcCheck(nvrtcGetPTX(prog, ptx.data()));
  std::vector<std::string> loweredNames;
  for (auto& entry : spec.entryPoints) {
    const char* temp;
    nvrtcCheck(nvrtcGetLoweredName(prog, entry.c_str(), &temp));
    loweredNames.push_back(std::string(temp));
  }

  nvrtcDestroyProgram(&prog);
  CUjit_option options[] = {
      CU_JIT_INFO_LOG_BUFFER,
      CU_JIT_INFO_LOG_BUFFER_SIZE_BYTES,
      CU_JIT_ERROR_LOG_BUFFER,
      CU_JIT_ERROR_LOG_BUFFER_SIZE_BYTES};
  char info[1024];
  char error[1024];
  uint32_t infoSize = sizeof(info);
  uint32_t errorSize = sizeof(error);
  void* values[] = {info, &infoSize, error, &errorSize};

  CUmodule module;
  auto loadResult = cuModuleLoadDataEx(
      &module, ptx.data(), sizeof(values) / sizeof(void*), options, values);
  if (loadResult != CUDA_SUCCESS) {
    LOG(ERROR) << "Load error " << errorSize << " " << infoSize;
    waveError(fmt::format("Error in load module: {} {}", info, error));
  }
  std::vector<CUfunction> funcs;
  for (auto& name : loweredNames) {
    funcs.emplace_back();
    CU_CHECK(cuModuleGetFunction(&funcs.back(), module, name.c_str()));
  }
  return std::make_shared<CompiledModuleImpl>(module, std::move(funcs));
}

void CompiledModuleImpl::launch(
    int32_t kernelIdx,
    int32_t numBlocks,
    int32_t numThreads,
    int32_t shared,
    Stream* stream,
    void** args) {
  auto result = cuLaunchKernel(
      kernels_[kernelIdx],
      numBlocks,
      1,
      1, // grid dim
      numThreads,
      1,
      1, // block dim
      shared,
      reinterpret_cast<CUstream>(stream->stream()->stream),
      args,
      0);
  CU_CHECK(result);
};

KernelInfo CompiledModuleImpl::info(int32_t kernelIdx) {
  KernelInfo info;
  auto f = kernels_[kernelIdx];
  cuFuncGetAttribute(&info.numRegs, CU_FUNC_ATTRIBUTE_NUM_REGS, f);
  cuFuncGetAttribute(
      &info.sharedMemory, CU_FUNC_ATTRIBUTE_SHARED_SIZE_BYTES, f);
  cuFuncGetAttribute(&info.localMemory, CU_FUNC_ATTRIBUTE_LOCAL_SIZE_BYTES, f);
  cuFuncGetAttribute(
      &info.maxThreadsPerBlock, CU_FUNC_ATTRIBUTE_MAX_THREADS_PER_BLOCK, f);
  int32_t max;
  cuOccupancyMaxActiveBlocksPerMultiprocessor(&max, f, 256, 0);
  info.maxOccupancy0 = max;
  cuOccupancyMaxActiveBlocksPerMultiprocessor(&max, f, 256, 256 * 32);
  info.maxOccupancy32 = max;
  return info;
}

} // namespace facebook::velox::wave
