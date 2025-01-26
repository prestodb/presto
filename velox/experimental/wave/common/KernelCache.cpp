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

#include "velox/common/caching/CachedFactory.h"
#include "velox/experimental/wave/common/Cuda.h"

#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/futures/Future.h>

namespace facebook::velox::wave {

using ModulePtr = std::shared_ptr<CompiledModule>;

static folly::CPUThreadPoolExecutor* compilerExecutor() {
  static std::unique_ptr<folly::CPUThreadPoolExecutor> pool =
      std::make_unique<folly::CPUThreadPoolExecutor>(10);
  return pool.get();
}

namespace {
class FutureCompiledModule : public CompiledModule {
 public:
  explicit FutureCompiledModule(folly::Future<ModulePtr> future)
      : future_(std::move(future)) {}

  void launch(
      int32_t kernelIdx,
      int32_t numBlocks,
      int32_t numThreads,
      int32_t shared,
      Stream* stream,
      void** args) override {
    ensureReady();
    module_->launch(kernelIdx, numBlocks, numThreads, shared, stream, args);
  }

  KernelInfo info(int32_t kernelIdx) override {
    ensureReady();
    return module_->info(kernelIdx);
  }

 private:
  void ensureReady() {
    std::lock_guard<std::mutex> l(mutex_);
    // 'module_' is a shared_ptr, so read is not atomic. Read inside the mutex.
    if (module_) {
      return;
    }
    module_ = std::move(future_).get();
  }

 private:
  std::mutex mutex_;
  ModulePtr module_;
  folly::Future<ModulePtr> future_;
};

using KernelPtr = CachedPtr<std::string, ModulePtr>;

class AsyncCompiledKernel : public CompiledKernel {
 public:
  explicit AsyncCompiledKernel(KernelPtr ptr) : ptr_(std::move(ptr)) {}

  void launch(
      int32_t kernelIdx,
      int32_t numBlocks,
      int32_t numThreads,
      int32_t shared,
      Stream* stream,
      void** args) override {
    (*ptr_)->launch(kernelIdx, numBlocks, numThreads, shared, stream, args);
  }

  KernelInfo info(int32_t kernelIdx) override {
    return (*ptr_)->info(kernelIdx);
  }

 private:
  KernelPtr ptr_;
};

class KernelGenerator {
 public:
  std::unique_ptr<ModulePtr>
  operator()(const std::string, const KernelGenFunc* gen, const void* stats) {
    using ModulePromise = folly::Promise<ModulePtr>;
    struct PromiseHolder {
      ModulePromise promise;
    };
    auto holder = std::make_shared<PromiseHolder>();

    auto future = holder->promise.getFuture();
    auto* device = currentDevice();
    compilerExecutor()->add([genCopy = *gen, holder, device]() {
      setDevice(device);
      auto spec = genCopy();
      auto module = CompiledModule::create(spec);
      holder->promise.setValue(module);
    });
    ModulePtr result =
        std::make_shared<FutureCompiledModule>(std::move(future));
    return std::make_unique<ModulePtr>(result);
  }
};

using KernelCache =
    CachedFactory<std::string, ModulePtr, KernelGenerator, KernelGenFunc>;

std::unique_ptr<KernelCache> makeCache() {
  auto generator = std::make_unique<KernelGenerator>();
  return std::make_unique<KernelCache>(
      std::make_unique<SimpleLRUCache<std::string, ModulePtr>>(1000),
      std::move(generator));
}

KernelCache& kernelCache() {
  static std::unique_ptr<KernelCache> cache = makeCache();
  return *cache;
}
} // namespace

//  static
std::unique_ptr<CompiledKernel> CompiledKernel::getKernel(
    const std::string& key,
    KernelGenFunc gen) {
  auto ptr = kernelCache().generate(key, &gen);
  return std::make_unique<AsyncCompiledKernel>(std::move(ptr));
}

} // namespace facebook::velox::wave
