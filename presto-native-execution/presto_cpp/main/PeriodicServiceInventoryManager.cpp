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
#include "presto_cpp/main/PeriodicServiceInventoryManager.h"
#include <folly/futures/Retrying.h>
#include <velox/common/memory/Memory.h>

namespace facebook::presto {
PeriodicServiceInventoryManager::PeriodicServiceInventoryManager(
    std::string address,
    int port,
    std::shared_ptr<CoordinatorDiscoverer> coordinatorDiscoverer,
    folly::SSLContextPtr sslContext,
    std::string id,
    uint64_t frequencyMs)
    : address_(std::move(address)),
      port_(port),
      coordinatorDiscoverer_(std::move(coordinatorDiscoverer)),
      sslContext_(std::move(sslContext)),
      id_(std::move(id)),
      frequencyMs_(frequencyMs),
      pool_(velox::memory::deprecatedAddDefaultLeafMemoryPool(id_)),
      eventBaseThread_(false /*autostart*/) {}

void PeriodicServiceInventoryManager::start() {
  eventBaseThread_.start(id_);
  sessionPool_ = std::make_unique<proxygen::SessionPool>(nullptr, 10);
  stopped_ = false;
  auto* eventBase = eventBaseThread_.getEventBase();
  eventBase->runOnDestruction([this] { sessionPool_.reset(); });
  eventBase->schedule([this]() { return sendRequest(); });
}

void PeriodicServiceInventoryManager::stop() {
  stopped_ = true;
  eventBaseThread_.stop();
}

void PeriodicServiceInventoryManager::sendRequest() {
  // stop() calls EventBase's destructor which executed all pending callbacks;
  // make sure not to do anything if that's the case
  if (stopped_) {
    return;
  }

  try {
    folly::SocketAddress newAddress = coordinatorDiscoverer_->getAddress();
    // Update service address after `updateServiceTimes()` attempts.
    if (updateServiceTimes() > 0 && (attempts_++) % updateServiceTimes() == 0) {
      newAddress = coordinatorDiscoverer_->updateAddress();
    }
    if (newAddress != serviceAddress_) {
      LOG(INFO) << "Service Inventory changed to " << newAddress.getAddressStr()
                << ":" << newAddress.getPort();
      std::swap(serviceAddress_, newAddress);
      client_ = std::make_shared<http::HttpClient>(
          eventBaseThread_.getEventBase(),
          sessionPool_.get(),
          serviceAddress_,
          std::chrono::milliseconds(10'000),
          std::chrono::milliseconds(0),
          pool_,
          sslContext_);
    }
  } catch (const std::exception& ex) {
    LOG(WARNING) << "Error occurred during updating service address: "
                 << ex.what();
    scheduleNext();
    return;
  }

  auto [request, body] = httpRequest();

  client_->sendRequest(request, body)
      .via(eventBaseThread_.getEventBase())
      .thenValue([this](auto response) {
        auto message = response->headers();
        // Treat both 202 and 204 as success.
        if (message->getStatusCode() != http::kHttpAccepted &&
            message->getStatusCode() != http::kHttpNoContent) {
          ++failedAttempts_;
          LOG(WARNING) << id_ << " failed: HTTP " << message->getStatusCode()
                       << " - " << response->dumpBodyChain();
        } else if (response->hasError()) {
          ++failedAttempts_;
          LOG(ERROR) << id_ << " failed: " << response->error();
        } else {
          failedAttempts_ = 0;
          LOG(INFO) << id_ << " succeeded: HTTP " << message->getStatusCode();
        }
      })
      .thenError(
          folly::tag_t<std::exception>{},
          [this](const std::exception& e) {
            ++failedAttempts_;
            LOG(WARNING) << id_ << " failed: " << e.what();
          })
      .thenTry([this](auto /*unused*/) { scheduleNext(); });
}

uint64_t PeriodicServiceInventoryManager::getDelayMs() {
  if (failedAttempts_ > 0 && retryFailed()) {
    // For failure cases, execute exponential back off to ping
    // coordinator with max back off time cap at 'frequencyMs_'.
    auto rng = folly::ThreadLocalPRNG();
    return folly::futures::detail::retryingJitteredExponentialBackoffDur(
               failedAttempts_,
               std::chrono::milliseconds(50),
               std::chrono::milliseconds(frequencyMs_),
               backOffjitterParam_,
               rng)
        .count();
  }

  // Adds some jitter for successful cases so that all workers does not ping
  // coordinator at the same time
  return frequencyMs_ - folly::Random::rand32(frequencyMs_ / 10);
}

void PeriodicServiceInventoryManager::scheduleNext() {
  if (stopped_) {
    return;
  }
  eventBaseThread_.getEventBase()->scheduleAt(
      [this]() { return sendRequest(); },
      std::chrono::steady_clock::now() +
          std::chrono::milliseconds(getDelayMs()));
}
} // namespace facebook::presto
