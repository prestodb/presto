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

#include "velox/buffer/Buffer.h"

#include "folly/Range.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/testutil/TestValue.h"
#include "velox/type/StringView.h"

#include <glog/logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

DECLARE_bool(velox_enable_memory_usage_track_in_default_memory_pool);

namespace facebook {
namespace velox {

static_assert(Buffer::is_pod_like_v<int64_t>, "");
static_assert(Buffer::is_pod_like_v<StringView>, "");
static_assert(Buffer::is_pod_like_v<folly::Range<const char*>>, "");
static_assert(Buffer::is_pod_like_v<velox::Range<const char*>>, "");
static_assert(!Buffer::is_pod_like_v<std::shared_ptr<int>>, "");

class BufferTest : public testing::Test {
 protected:
  static void SetUpTestCase() {
    FLAGS_velox_enable_memory_usage_track_in_default_memory_pool = true;
  }

  void SetUp() override {
    pool_ = memoryManager_.addLeafPool("BufferTest");
  }

  memory::MemoryManager memoryManager_;
  std::shared_ptr<memory::MemoryPool> pool_;
};

TEST_F(BufferTest, testAlignedBuffer) {
  static constexpr int32_t kHeaderSize = sizeof(AlignedBuffer);
  const int32_t size = 1024 * 7;
  const int32_t sizeWithHeader = size + kHeaderSize;
  BufferPtr other;
  {
    const char* testString = "1234567\0";
    const int32_t testStringLength = strlen(testString);
    BufferPtr buffer = AlignedBuffer::allocate<char>(size, pool_.get(), 'i');
    EXPECT_EQ(buffer->as<char>()[0], 'i');
    EXPECT_TRUE(buffer->isMutable());
    EXPECT_EQ(buffer->size(), size);
    EXPECT_GE(buffer->capacity(), size);
    buffer->setSize(buffer->capacity());
    memcpy(
        buffer->asMutable<uint8_t>() + buffer->capacity() - testStringLength,
        testString,
        testStringLength);
    other = buffer;
    EXPECT_EQ(pool_->usedBytes(), pool_->preferredSize(sizeWithHeader));

    AlignedBuffer::reallocate<char>(&other, size * 3, 'e');
    EXPECT_NE(other, buffer);

    // No longer multiply referenced.
    EXPECT_GE(other->capacity(), 3 * size);
    EXPECT_EQ(other->size(), 3 * size);
    EXPECT_EQ(
        memcmp(
            other->as<uint8_t>() + buffer->capacity() - testStringLength,
            testString,
            testStringLength),
        0);
    EXPECT_EQ(other->as<char>()[buffer->capacity()], 'e');
    EXPECT_EQ(
        pool_->usedBytes(),
        pool_->preferredSize(sizeWithHeader) +
            pool_->preferredSize(3 * size + kHeaderSize));
  }
  EXPECT_EQ(pool_->usedBytes(), pool_->preferredSize(3 * size + kHeaderSize));
  other = nullptr;
  BufferPtr bits = AlignedBuffer::allocate<bool>(65, pool_.get(), true);
  EXPECT_EQ(bits->size(), 9);
  EXPECT_EQ(bits->as<uint8_t>()[8], 0xff);
  bits = nullptr;
  EXPECT_EQ(pool_->usedBytes(), 0);
}

TEST_F(BufferTest, testAsRange) {
  // Simple 2 element vector.
  std::vector<uint8_t> testData({5, 255});
  BufferPtr buffer =
      AlignedBuffer::allocate<uint8_t>(2 /*numElements*/, pool_.get());

  memcpy(buffer->asMutable<uint8_t>(), testData.data(), testData.size());

  Range<uint8_t> range = buffer->asRange<uint8_t>();
  MutableRange<uint8_t> mutRange = buffer->asMutableRange<uint8_t>();
  EXPECT_EQ(5, range[0]);
  EXPECT_EQ(255, range[1]);

  EXPECT_EQ(5, mutRange[0]);
  EXPECT_EQ(255, mutRange[1]);
}

TEST_F(BufferTest, testAlignedBufferPrint) {
  // We'll only put non-default values for the first 2 bytes. Note how below
  // in the string it has 05 ff which corresponds to these.
  std::vector<uint8_t> testData({5, 255});

  // We want at least 4 bytes, since we're going to see what unallocated prints
  // as also
  BufferPtr buffer = AlignedBuffer::allocate<uint8_t>(
      4 /*numElements*/, pool_.get(), 1 /*value*/);

  memcpy(buffer->asMutable<uint8_t>(), testData.data(), testData.size());

  buffer->setSize(2);

  std::stringstream bufferAsStream;
  bufferAsStream << *buffer;
  std::string bufferAsString = bufferAsStream.str();

  // Allocated will actually be more than we asked for, so I'm not including the
  // full string output - just the pieces we've guaranteed.
  ASSERT_THAT(
      bufferAsString,
      testing::StartsWith(
          "{ size: 2, capacity: " + std::to_string(buffer->capacity()) +
          ", refCount: 1, unique: true, isMutable: true, isView: false,"
          " data: [ 05 ff || <-- size | remaining allocated --> || 01 01 "));
  ASSERT_THAT(bufferAsString, testing::EndsWith(" ] }"));
}

TEST_F(BufferTest, testReallocate) {
  std::vector<BufferPtr> buffers;

  for (int32_t i = 0; i < 1000; ++i) {
    buffers.push_back(AlignedBuffer::allocate<char>(i, pool_.get()));
  }
  // Adjust sizes up and down, check that we have 0 at the end.
  int32_t numInPlace = 0;
  int32_t numMoved = 0;
  for (int32_t i = 0; i < buffers.size(); ++i) {
    size_t oldSize = buffers[i]->size();
    auto ptr = buffers[i].get();
    if (i % 10 == 0) {
      AlignedBuffer::reallocate<char>(&buffers[i], i + 10000);
      EXPECT_EQ(buffers[i]->size(), i + 10000);
    } else if (i % 3 == 0) {
      AlignedBuffer::reallocate<char>(&buffers[i], std::max(50, i / 2));
      EXPECT_LE(buffers[i]->size(), buffers[i]->capacity());
    } else {
      size_t capacity = buffers[i]->capacity();
      buffers[i]->setSize(capacity);
      std::string newData = "12345678";
      newData.resize(capacity + 10);
      char* copy =
          AlignedBuffer::appendTo(&buffers[i], newData.data(), newData.size());
      EXPECT_EQ(memcmp(copy, newData.data(), 8), 0);
    }
    if (buffers[i].get() == ptr) {
      ++numInPlace;
    } else {
      ++numMoved;
    }
  }
  buffers.clear();
  EXPECT_EQ(pool_->usedBytes(), 0);
  EXPECT_GT(numInPlace, 0);
  EXPECT_GT(numMoved, 0);
}

TEST_F(BufferTest, testReallocateNoReuse) {
  // This test checks that regardless of how we resize a Buffer in reallocate
  // (up, down, the same) as long as we hit MemoryPool::reallocate,  the Buffer
  // always points to a new location in memory. If this test fails, it's not
  // necessarily a problem, but it's worth looking at optimizations we could do
  // in reallocate when the pointer doesn't change.

  enum BufferResizeOption {
    BIGGER,
    SMALLER,
    SAME,
  };

  auto test = [&](BufferResizeOption bufferResizeOption,
                  bool useMmapAllocator) {
    memory::MemoryManagerOptions options;
    options.useMmapAllocator = useMmapAllocator;
    options.allocatorCapacity = 1024 * 1024;
    memory::MemoryManager memoryManager(options);

    auto pool = memoryManager.addLeafPool("testReallocateNoReuse");

    const size_t originalBufferSize = 10;
    auto buffer = AlignedBuffer::allocate<char>(originalBufferSize, pool.get());
    auto* originalBufferPtr = buffer.get();

    size_t newSize;
    switch (bufferResizeOption) {
      case SMALLER:
        newSize = originalBufferSize - 1;
        break;
      case SAME:
        newSize = originalBufferSize;
        break;
      case BIGGER:
        // Make sure the new size is large enough that we hit
        // MemoryPoolImpl::reallocate.
        newSize = buffer->capacity() + 1;
        break;
      default:
        VELOX_FAIL("Unexpected buffer resize option");
    }

    AlignedBuffer::reallocate<char>(&buffer, newSize);

    EXPECT_NE(buffer.get(), originalBufferPtr);
  };

  test(SMALLER, true);
  test(SAME, true);
  test(BIGGER, true);

  test(SMALLER, false);
  test(SAME, false);
  test(BIGGER, false);
}

DEBUG_ONLY_TEST_F(BufferTest, testReallocateFails) {
  // Reallocating a buffer can cause an exception to be thrown e.g. if we
  // run out of memory.  If the buffer is left in an invalid state this can
  // cause crahses, e.g. if VectorSaver attempts to write out a Vector that
  // was in the midst of resizing.  This test verifies the buffer is valid at
  // different points in the exception's lifecycle.

  const size_t bufferSize = 10;
  auto buffer = AlignedBuffer::allocate<char>(bufferSize, pool_.get());

  ::memset(buffer->asMutable<char>(), 'a', bufferSize);

  common::testutil::TestValue::enable();

  const std::string kErrorMessage = "Expected out of memory exception";
  SCOPED_TESTVALUE_SET(
      "facebook::velox::memory::MemoryPoolImpl::reserveThreadSafe",
      std::function<void(memory::MemoryPool*)>([&](memory::MemoryPool*) {
        VELOX_MEM_POOL_CAP_EXCEEDED(kErrorMessage);
      }));

  {
    ExceptionContextSetter setter(
        {.messageFunc = [](VeloxException::Type,
                           void* untypedArg) -> std::string {
           // Validate that the buffer is still valid at the point
           // the exception is thrown.
           auto bufferArg = *static_cast<BufferPtr*>(untypedArg);

           const auto* bufferContents = bufferArg->as<char>();
           VELOX_CHECK_EQ(bufferArg->size(), 10);
           for (int i = 0; i < 10; i++) {
             VELOX_CHECK_EQ(bufferContents[i], 'a');
           }

           return "Exception context message func called.";
         },
         .arg = &buffer});

    VELOX_ASSERT_THROW_CODE(
        AlignedBuffer::reallocate<char>(
            &buffer, pool_->availableReservation() + 1),
        error_code::kMemCapExceeded,
        kErrorMessage);
  }

  // Validate the buffer is valid after the exception is caught.
  const auto* bufferContents = buffer->as<char>();
  VELOX_CHECK_EQ(buffer->size(), bufferSize);
  for (int i = 0; i < bufferSize; i++) {
    VELOX_CHECK_EQ(bufferContents[i], 'a');
  }
}

struct MockCachePin {
  void addRef() {
    ++pinCount;
  }

  void release() {
    --pinCount;
  }

  int32_t pinCount = 0;
};

TEST_F(BufferTest, testBufferView) {
  MockCachePin pin;
  const char* data = "12345678\0";
  BufferPtr buffer = BufferView<MockCachePin&>::create(
      reinterpret_cast<const uint8_t*>(data), sizeof(data), pin);
  EXPECT_EQ(buffer->size(), sizeof(data));
  EXPECT_EQ(buffer->capacity(), sizeof(data));
  EXPECT_EQ(pin.pinCount, 1);
  EXPECT_FALSE(buffer->isMutable());
  {
    BufferPtr other = buffer;
    EXPECT_EQ(pin.pinCount, 1);
    EXPECT_FALSE(buffer->unique());
    EXPECT_EQ(memcmp(data, other->as<uint8_t>(), strlen(data)), 0);
  }
  EXPECT_TRUE(buffer->unique());
  buffer = nullptr;
  EXPECT_EQ(pin.pinCount, 0);
}

struct NonPOD {
  static int constructed;
  static int destructed;

  static void clearStats() {
    constructed = 0;
    destructed = 0;
  }

  int x;

  NonPOD(int x = 123) : x(x) {
    ++constructed;
  }

  NonPOD(const NonPOD& other) : x(other.x) {
    ++constructed;
  }

  NonPOD(NonPOD&& other) noexcept : x(other.x) {
    ++constructed;
    // gobble the data
    other.x = -2;
  }

  NonPOD& operator=(const NonPOD&) = default;
  NonPOD& operator=(NonPOD&&) = default;

  ~NonPOD() {
    // gobble the data
    x = -1;
    ++destructed;
  }
};

int NonPOD::constructed = 0;
int NonPOD::destructed = 0;

TEST_F(BufferTest, testNonPOD) {
  NonPOD::clearStats();
  auto buf = AlignedBuffer::allocate<NonPOD>(10, pool_.get());
  EXPECT_EQ(sizeof(NonPOD) * 10, buf->size());
  EXPECT_EQ(NonPOD::constructed, 10);
  EXPECT_EQ(NonPOD::destructed, 0);
  // recasting doesn't work, but void access does
#ifndef NDEBUG
  EXPECT_THROW(buf->as<char>(), VeloxException);
#endif
  buf->as<void>();

  for (int i = 0; i < 10; ++i) {
    // default-initialized
    EXPECT_EQ(buf->asMutable<NonPOD>()[i].x, 123);
    buf->asMutable<NonPOD>()[i].x = i;
  }

  // shrink by setting size
  buf->setSize(9 * sizeof(NonPOD));
  for (int i = 0; i < 9; ++i) {
    EXPECT_EQ(buf->as<NonPOD>()[i].x, i);
  }
  EXPECT_EQ(NonPOD::constructed, 10);
  EXPECT_EQ(NonPOD::destructed, 1);

  // shrink by reallocating, we don't enforce in-place shrink yet, thus checking
  // only relative difference between constructed and destructed
  AlignedBuffer::reallocate<NonPOD>(&buf, 7);
  EXPECT_EQ(NonPOD::constructed - NonPOD::destructed, 7);
  for (int i = 0; i < 7; ++i) {
    EXPECT_EQ(buf->as<NonPOD>()[i].x, i);
  }

  // grow out-of-place
  {
    const void* prev = buf->as<void>();
    AlignedBuffer::reallocate<NonPOD>(&buf, 20);
    // make sure we're testing out-of-place reallocation
    EXPECT_NE(prev, buf->as<void>());
  }
  EXPECT_EQ(NonPOD::constructed - NonPOD::destructed, 20);
  for (int i = 0; i < 7; ++i) {
    EXPECT_EQ(buf->as<NonPOD>()[i].x, i);
  }

  // grow in-place
  {
    EXPECT_LE(buf->size() + sizeof(NonPOD), buf->capacity());
    const void* prev = buf->as<void>();
    AlignedBuffer::reallocate<NonPOD>(&buf, 21);
    // make sure we're testing in-place allocation, the test size might need to
    // be adjusted if the "preferred size formula" changes
    EXPECT_EQ(prev, buf->as<void>());
  }
  EXPECT_EQ(NonPOD::constructed - NonPOD::destructed, 21);
  for (int i = 0; i < 7; ++i) {
    EXPECT_EQ(buf->as<NonPOD>()[i].x, i);
  }

  // grow in-place by setting size
  EXPECT_LE(buf->size() + sizeof(NonPOD), buf->capacity());
  buf->setSize(22 * sizeof(NonPOD));
  EXPECT_EQ(NonPOD::constructed - NonPOD::destructed, 22);
  for (int i = 0; i < 7; ++i) {
    EXPECT_EQ(buf->as<NonPOD>()[i].x, i);
  }

  // free stuff
  buf = nullptr;
  EXPECT_EQ(NonPOD::constructed, NonPOD::destructed);
}

TEST_F(BufferTest, testNonPODMemoryUsage) {
  using T = std::shared_ptr<void>;
  const int64_t currentBytes = pool_->usedBytes();
  { auto buffer = AlignedBuffer::allocate<T>(0, pool_.get()); }
  EXPECT_EQ(pool_->usedBytes(), currentBytes);
}

TEST_F(BufferTest, testAllocateSizeOverflow) {
  EXPECT_THROW(
      AlignedBuffer::allocate<int64_t>(1ull << 62, pool_.get()),
      VeloxException);
  auto buf = AlignedBuffer::allocate<int64_t>(8, pool_.get());
  EXPECT_THROW(
      AlignedBuffer::reallocate<int64_t>(&buf, 1ull << 62), VeloxException);
}

TEST_F(BufferTest, sliceBigintBuffer) {
  auto bufferPtr = AlignedBuffer::allocate<int64_t>(10, pool_.get());
  auto sliceBufferPtr = Buffer::slice<int64_t>(bufferPtr, 1, 5, pool_.get());
  ASSERT_TRUE(sliceBufferPtr->isView());
  ASSERT_EQ(sliceBufferPtr->size(), 40); // 5 * type size of int64_t.
  ASSERT_EQ(sliceBufferPtr->as<int64_t>(), bufferPtr->as<int64_t>() + 1);

  VELOX_ASSERT_THROW(
      Buffer::slice<int64_t>(bufferPtr, 11, 1, pool_.get()),
      "Offset must be less than or equal to 10.");
  VELOX_ASSERT_THROW(
      Buffer::slice<int64_t>(bufferPtr, 5, 6, pool_.get()),
      "Length must be less than or equal to 5.");
  VELOX_ASSERT_THROW(
      Buffer::slice<int64_t>(nullptr, 5, 6, pool_.get()),
      "Buffer must not be null.");
}

TEST_F(BufferTest, sliceBooleanBuffer) {
  auto bufferPtr = AlignedBuffer::allocate<bool>(16, pool_.get());
  auto data = bufferPtr->asMutableRange<bool>();
  for (int i = 0; i < 16; ++i) {
    data[i] = (i % 2 != 0);
  }
  auto sliceBufferPtr = Buffer::slice<bool>(bufferPtr, 8, 8, pool_.get());
  ASSERT_TRUE(sliceBufferPtr->isView());
  ASSERT_EQ(sliceBufferPtr->as<bool>(), bufferPtr->as<bool>() + 1);

  sliceBufferPtr = Buffer::slice<bool>(bufferPtr, 5, 5, pool_.get());
  ASSERT_FALSE(sliceBufferPtr->isView());
  auto sliceData = sliceBufferPtr->asRange<bool>();
  for (int i = 0; i < 5; ++i) {
    ASSERT_EQ(sliceData[i], i % 2 == 0);
  }
  VELOX_ASSERT_THROW(
      Buffer::slice<bool>(bufferPtr, 5, 6, nullptr), "Pool must not be null.");
}

} // namespace velox
} // namespace facebook
