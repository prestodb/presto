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

// Basic memory arena.
//
// Standard usage:
//   char* pos = arena->reserve(100)
//   write data to [pos, pos + 100)
//   pos = arena->reserve(27);
//   write data to [pos, pos + 27)
//   and so on

#pragma once

#include <cstdint>
#include <cstring>
#include <memory>
#include <string_view>
#include <vector>

namespace facebook::velox {

// Internally manages memory in chunks. Releases memory only upon destruction.
// Arena is NOT threadsafe: external locking is required. All functions in this
// class are expected to be used in tight loops, so we inline everything.
class Arena {
 public:
  Arena(int64_t initial_chunk_size = kMinChunkSize) {
    addChunk(initial_chunk_size);
    reserveEnd_ = pos_;
  }

  // Returns a pointer to a block of memory of size bytes that can be written
  // to, and guarantees for the lifetime of *this that that region will remain
  // valid. Does NOT guarantee that the region is initially 0'd.
  char* reserve(int64_t bytes) {
    if (reserveEnd_ + bytes <= chunkEnd_) {
      pos_ = reserveEnd_;
      reserveEnd_ += bytes;
    } else {
      addChunk(bytes);
    }
    return pos_;
  }

  // Copies |data| into the chunk, returning a view to the copied data.
  std::string_view writeString(std::string_view data) {
    char* pos = reserve(data.size());
    memcpy(pos, data.data(), data.size());
    return {pos, data.size()};
  }

 private:
  static constexpr int64_t kMinChunkSize = 1LL << 20;

  void addChunk(int64_t bytes) {
    const int64_t chunkSize = std::max(bytes, kMinChunkSize);
    chunks_.emplace_back(new char[chunkSize]);
    pos_ = chunks_.back().get();
    chunkEnd_ = pos_ + chunkSize;
    reserveEnd_ = pos_ + bytes;
  }

  char* chunkEnd_;
  char* pos_;
  char* reserveEnd_;
  std::vector<std::unique_ptr<char[]>> chunks_;
};

} //  namespace facebook::velox
