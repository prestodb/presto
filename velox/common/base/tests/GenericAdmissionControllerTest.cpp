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

#include "velox/common/base/GenericAdmissionController.h"
#include <gtest/gtest.h>
#include <atomic>
#include "velox/common/base/VeloxException.h"
#include "velox/common/base/tests/GTestUtils.h"

using namespace facebook::velox;
namespace facebook::velox::common {
TEST(GenericAdmissionController, basic) {
  const uint64_t kLimit = 100000;
  GenericAdmissionController::Config config;
  config.maxLimit = kLimit;
  GenericAdmissionController admissionController(config);
  EXPECT_EQ(admissionController.currentResourceUsage(), 0);

  admissionController.accept(100);
  EXPECT_EQ(admissionController.currentResourceUsage(), 100);

  admissionController.accept(100);
  EXPECT_EQ(admissionController.currentResourceUsage(), 200);

  admissionController.release(100);
  EXPECT_EQ(admissionController.currentResourceUsage(), 100);

  VELOX_ASSERT_THROW(
      admissionController.release(101),
      "Cannot release more units than have been acquired");

  VELOX_ASSERT_THROW(
      admissionController.accept(kLimit + 1),
      "A single request cannot exceed the max limit");
}

TEST(GenericAdmissionController, multiThreaded) {
  // Ensure that resource usage never exceeds the limit set in the admission
  // controller.
  const uint64_t kLimit = 10;
  std::atomic_uint64_t currentUsage{0};
  GenericAdmissionController::Config config;
  config.maxLimit = kLimit;
  GenericAdmissionController admissionController(config);

  std::vector<std::thread> threads;
  for (int i = 0; i < 20; i++) {
    threads.push_back(std::thread([&]() {
      for (int j = 0; j < 1000; j++) {
        admissionController.accept(1);
        uint64_t curr = currentUsage.fetch_add(1);
        ASSERT_LE(curr + 1, kLimit);
        currentUsage--;
        admissionController.release(1);
      }
    }));
  }
  for (auto& thread : threads) {
    thread.join();
  }
}
} // namespace facebook::velox::common
