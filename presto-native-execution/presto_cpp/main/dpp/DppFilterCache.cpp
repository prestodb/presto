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
#include "presto_cpp/main/dpp/DppFilterCache.h"

#include <fmt/format.h>
#include <glog/logging.h>

#include <cstring>
#include <limits>

namespace facebook::presto::dpp {

namespace {

struct SingletonState {
  std::mutex mutex;
  std::unique_ptr<DppFilterCache> instance;
  int64_t maxBytes{0};
};

SingletonState& singletonState() {
  static SingletonState state;
  return state;
}

constexpr int64_t kDefaultMaxBytes = 2LL << 30; // 2 GB

} // namespace

void DppFilterCache::initialize(int64_t maxBytes) {
  auto& state = singletonState();
  std::lock_guard<std::mutex> guard(state.mutex);
  if (state.instance != nullptr) {
    if (state.maxBytes != maxBytes) {
      LOG(WARNING) << "DppFilterCache already initialized with "
                   << state.maxBytes
                   << " bytes; ignoring re-initialization request for "
                   << maxBytes << " bytes";
    }
    return;
  }
  state.maxBytes = maxBytes;
  state.instance.reset(new DppFilterCache(maxBytes));
}

DppFilterCache* DppFilterCache::getInstance() {
  auto& state = singletonState();
  std::lock_guard<std::mutex> guard(state.mutex);
  if (state.instance == nullptr) {
    state.maxBytes = kDefaultMaxBytes;
    state.instance.reset(new DppFilterCache(kDefaultMaxBytes));
  }
  return state.instance.get();
}

void DppFilterCache::testingReset() {
  auto& state = singletonState();
  std::lock_guard<std::mutex> guard(state.mutex);
  state.instance.reset();
  state.maxBytes = 0;
}

DppFilterCache::DppFilterCache(int64_t maxBytes) {
  rootPool_ =
      velox::memory::MemoryManager::getInstance()->addRootPool(
          "dppFilterCache", maxBytes, DppFilterCacheReclaimer::create());
}

int64_t DppFilterCache::maxCapacity() const {
  return rootPool_->maxCapacity();
}

int64_t DppFilterCache::currentBytes() const {
  return rootPool_->reservedBytes();
}

std::shared_ptr<velox::memory::MemoryPool> DppFilterCache::createTaskPool(
    const std::string& taskId) {
  // Counter avoids leaf-child name collisions when findOrCreateTask races
  // for the same taskId.
  static std::atomic<uint64_t> nextSeq{0};
  return rootPool_->addLeafChild(
      fmt::format("task.{}.{}", taskId, nextSeq.fetch_add(1)));
}

void DppFilterCache::registerFilter(
    PrestoTask* task,
    const std::string& filterId,
    int64_t size,
    EvictCallback evictCallback) {
  std::lock_guard<std::mutex> guard(mutex_);
  TaskFilterKey key{task, filterId};
  auto it = index_.find(key);
  if (it != index_.end()) {
    it->second->size = size;
    it->second->evict = std::move(evictCallback);
    lru_.splice(lru_.end(), lru_, it->second);
    return;
  }
  lru_.push_back({task, filterId, size, std::move(evictCallback)});
  auto last = std::prev(lru_.end());
  index_[key] = last;
}

void DppFilterCache::touch(PrestoTask* task, const std::string& filterId) {
  std::lock_guard<std::mutex> guard(mutex_);
  auto it = index_.find({task, filterId});
  if (it == index_.end()) {
    return;
  }
  lru_.splice(lru_.end(), lru_, it->second);
}

void DppFilterCache::forget(PrestoTask* task, const std::string& filterId) {
  std::lock_guard<std::mutex> guard(mutex_);
  auto it = index_.find({task, filterId});
  if (it == index_.end()) {
    return;
  }
  lru_.erase(it->second);
  index_.erase(it);
}

void DppFilterCache::forgetTask(PrestoTask* task) {
  std::lock_guard<std::mutex> guard(mutex_);
  for (auto it = index_.begin(); it != index_.end();) {
    if (it->first.first == task) {
      lru_.erase(it->second);
      it = index_.erase(it);
    } else {
      ++it;
    }
  }
}

uint64_t DppFilterCache::evictUpTo(uint64_t targetBytes) {
  uint64_t freed = 0;
  while (freed < targetBytes) {
    Entry victim;
    {
      std::lock_guard<std::mutex> guard(mutex_);
      if (lru_.empty()) {
        return freed;
      }
      victim = std::move(lru_.front());
      lru_.pop_front();
      index_.erase({victim.task, victim.filterId});
    }
    bool evicted = false;
    try {
      evicted = victim.evict(victim.filterId);
    } catch (const std::exception& e) {
      LOG(WARNING) << "DppFilterCache evict callback failed for filter '"
                   << victim.filterId << "': " << e.what();
    }
    if (evicted) {
      freed += victim.size;
    }
  }
  return freed;
}

std::unique_ptr<velox::memory::MemoryReclaimer>
DppFilterCacheReclaimer::create() {
  return std::unique_ptr<MemoryReclaimer>(new DppFilterCacheReclaimer());
}

bool DppFilterCacheReclaimer::reclaimableBytes(
    const velox::memory::MemoryPool& pool,
    uint64_t& reclaimableBytes) const {
  reclaimableBytes = pool.reservedBytes();
  return reclaimableBytes > 0;
}

uint64_t DppFilterCacheReclaimer::reclaim(
    velox::memory::MemoryPool* /*pool*/,
    uint64_t targetBytes,
    uint64_t /*maxWaitMs*/,
    Stats& stats) {
  const auto target = targetBytes == 0
      ? std::numeric_limits<uint64_t>::max()
      : targetBytes;
  const auto freed = DppFilterCache::getInstance()->evictUpTo(target);
  stats.reclaimedBytes += freed;
  return freed;
}

std::string serializeTupleDomain(
    const protocol::TupleDomain<std::string>& tupleDomain) {
  nlohmann::json j = tupleDomain;
  return j.dump();
}

protocol::TupleDomain<std::string> deserializeTupleDomain(
    const char* data,
    size_t size) {
  auto j = nlohmann::json::parse(data, data + size);
  return j.get<protocol::TupleDomain<std::string>>();
}

} // namespace facebook::presto::dpp
