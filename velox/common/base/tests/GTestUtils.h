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

#pragma once

#include <gtest/gtest.h>

// gtest v1.10 deprecated *_TEST_CASE in favor of *_TEST_SUITE. These
// macros are provided for portability between different gtest versions.
#ifdef TYPED_TEST_SUITE
#define VELOX_TYPED_TEST_SUITE TYPED_TEST_SUITE
#else
#define VELOX_TYPED_TEST_SUITE TYPED_TEST_CASE
#endif

#ifdef INSTANTIATE_TEST_SUITE_P
#define VELOX_INSTANTIATE_TEST_SUITE_P INSTANTIATE_TEST_SUITE_P
#else
#define VELOX_INSTANTIATE_TEST_SUITE_P INSTANTIATE_TEST_CASE_P
#endif

#define VELOX_ASSERT_THROW(expression, errorMessage)                 \
  try {                                                              \
    (expression);                                                    \
    FAIL() << "Expected an exception";                               \
  } catch (const VeloxException& e) {                                \
    ASSERT_TRUE(e.message().find(errorMessage) != std::string::npos) \
        << "Expected error message to contain '" << errorMessage     \
        << "', but received '" << e.message() << "'.";               \
  }
