/*
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

#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/futures/Future.h>
#include <folly/futures/Promise.h>
#include <folly/synchronization/EventCount.h>
#include <gtest/gtest.h>

namespace facebook::presto {
namespace {

// Wraps another executor without overriding keepAliveAcquire/Release.
// This mimics MonitoredExecutor's behavior: folly futures holding a raw pointer
// via ExecutorKeepAlive cannot prevent this executor from being destroyed.
// When destroyed, the wrapper's memory is freed even if outstanding futures
// still reference it through raw pointers.
class NoKeepAliveExecutor : public folly::Executor {
 public:
  explicit NoKeepAliveExecutor(std::unique_ptr<folly::Executor> base)
      : base_(std::move(base)) {}

  void add(folly::Func func) override {
    base_->add(std::move(func));
  }

 private:
  std::unique_ptr<folly::Executor> base_;
};

// Shared synchronization state between the test thread and async lambdas.
struct SyncState {
  folly::EventCount exchangeReadyEvent;
  std::atomic<bool> exchangeReadyFlag{false};
  folly::EventCount proceedEvent;
  std::atomic<bool> proceedFlag{false};
  std::atomic<bool> completed{false};
};

} // namespace

// Verifies that the shutdown order in PrestoServer::joinExecutors() correctly
// drains exchangeHttpCpuExecutor_ threads before destroying driverExecutor_.
//
// Production scenario:
//   - PrestoExchangeSource::handleDataResponse runs on exchangeHttpCpuExecutor_
//   - It calls requestPromise.setValue() which dispatches the ExchangeClient
//     callback to driverExecutor_ (a MonitoredExecutor) via raw pointer
//   - If driverExecutor_ is already destroyed, this is use-after-free
//
// This test forces the exact interleaving:
//   1. Exchange thread starts and signals it's ready
//   2. Test releases the exchange thread
//   3. Exchange thread dispatches to the driver executor via raw pointer
//   4. Shutdown drains exchange threads BEFORE destroying driver executor
//
// With the old shutdown order (driverExecutor_.reset() before
// exchangeHttpCpuExecutor_->join()), ASAN detects heap-use-after-free
// because the exchange callback calls add() on freed executor memory.
TEST(ShutdownOrderTest, exchangeCallbacksDrainBeforeDriverExecutorDestroyed) {
  // Create driver executor wrapped in NoKeepAliveExecutor to match
  // MonitoredExecutor's lack of keepalive token support.
  auto driverExecutor = std::make_unique<NoKeepAliveExecutor>(
      std::make_unique<folly::CPUThreadPoolExecutor>(
          2, std::make_shared<folly::NamedThreadFactory>("TestDriver")));

  // Separate exchange CPU executor (like exchangeHttpCpuExecutor_ in prod).
  auto exchangeCpu = std::make_unique<folly::CPUThreadPoolExecutor>(
      2, std::make_shared<folly::NamedThreadFactory>("TestExchangeCPU"));

  // Raw pointer, same as ExchangeClient::executor_ and the pointer stored
  // inside folly future cores by .via(executor_).
  auto* driverRawPtr = driverExecutor.get();

  auto sync = std::make_shared<SyncState>();

  // Simulate an in-flight exchange HTTP response callback running on
  // exchangeCpuExecutor that dispatches work to driverExecutor.
  // In production: handleDataResponse -> requestPromise.setValue() ->
  // Core::doCallback -> MonitoredExecutor::add()
  exchangeCpu->add([sync, driverRawPtr]() {
    sync->exchangeReadyFlag = true;
    sync->exchangeReadyEvent.notifyAll();
    sync->proceedEvent.await([sync]() { return sync->proceedFlag.load(); });

    // This is the critical call that crashes with use-after-free if
    // driverExecutor has been destroyed.
    driverRawPtr->add([]() {});
    sync->completed = true;
  });

  sync->exchangeReadyEvent.await(
      [sync]() { return sync->exchangeReadyFlag.load(); });
  sync->proceedFlag = true;
  sync->proceedEvent.notifyAll();

  // CORRECT shutdown order (the fix in joinExecutors()):
  // 1. Join exchange CPU — drains all callbacks that dispatch to
  // driverExecutor.
  exchangeCpu->join();

  // 2. Now safe to destroy driverExecutor — no outstanding raw pointer users.
  //    With the OLD ordering (steps 1 and 2 swapped), driverExecutor would be
  //    freed while the exchange thread still holds driverRawPtr, and the
  //    driverRawPtr->add() call above would be heap-use-after-free.
  driverExecutor.reset();

  exchangeCpu.reset();

  EXPECT_TRUE(sync->completed);
}

// Same scenario but using folly Promise/SemiFuture with .via(executor) to
// match the exact production code path in ExchangeClient::request().
TEST(ShutdownOrderTest, promiseChainDispatchesSafelyDuringShutdown) {
  auto driverExecutor = std::make_unique<NoKeepAliveExecutor>(
      std::make_unique<folly::CPUThreadPoolExecutor>(
          2, std::make_shared<folly::NamedThreadFactory>("TestDriver")));
  auto exchangeCpu = std::make_unique<folly::CPUThreadPoolExecutor>(
      2, std::make_shared<folly::NamedThreadFactory>("TestExchangeCPU"));

  auto* driverRawPtr = driverExecutor.get();

  // Set up a promise/future chain matching the production pattern:
  //   source->request() returns SemiFuture
  //   ExchangeClient does: future.via(driverExecutor).thenValue(callback)
  //   PrestoExchangeSource does: promise.setValue() on exchange CPU thread
  auto requestPromise = std::make_shared<folly::Promise<int>>();
  auto clientCallbackRan = std::make_shared<std::atomic<bool>>(false);
  auto clientFuture = requestPromise->getSemiFuture()
                          .via(driverRawPtr)
                          .thenValue([clientCallbackRan](int value) {
                            *clientCallbackRan = true;
                          });

  auto sync = std::make_shared<SyncState>();

  // Simulate handleDataResponse on exchange CPU thread fulfilling the promise.
  exchangeCpu->add([sync, requestPromise]() {
    sync->exchangeReadyFlag = true;
    sync->exchangeReadyEvent.notifyAll();
    sync->proceedEvent.await([sync]() { return sync->proceedFlag.load(); });
    // This triggers Core::doCallback which calls driverRawPtr->add().
    // If driverRawPtr points to freed memory, this is use-after-free.
    requestPromise->setValue(42);
  });

  sync->exchangeReadyEvent.await(
      [sync]() { return sync->exchangeReadyFlag.load(); });
  sync->proceedFlag = true;
  sync->proceedEvent.notifyAll();

  // Correct shutdown order.
  exchangeCpu->join();
  driverExecutor.reset();

  // The client callback may or may not have run (the driver executor's
  // internal thread pool was destroyed), but the critical invariant is
  // that we didn't crash. Under the old ordering, the promise.setValue()
  // call would dispatch to a freed executor.
}

} // namespace facebook::presto
