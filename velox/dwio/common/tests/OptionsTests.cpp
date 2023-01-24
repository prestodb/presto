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
#include <gtest/gtest.h>
#include "velox/dwio/common/Options.h"

using namespace ::testing;
using namespace facebook::velox::dwio::common;

TEST(OptionsTests, defaultAppendRowNumberColumnTest) {
  // appendRowNumberColumn flag should be false by default
  RowReaderOptions rowReaderOptions;
  ASSERT_EQ(false, rowReaderOptions.getAppendRowNumberColumn());
}

TEST(OptionsTests, setAppendRowNumberColumnToTrueTest) {
  RowReaderOptions rowReaderOptions;
  rowReaderOptions.setAppendRowNumberColumn(true);
  ASSERT_EQ(true, rowReaderOptions.getAppendRowNumberColumn());
}

TEST(OptionsTests, testAppendRowNumberColumnInCopy) {
  RowReaderOptions rowReaderOptions;
  RowReaderOptions rowReaderOptionsCopy{rowReaderOptions};
  ASSERT_EQ(false, rowReaderOptionsCopy.getAppendRowNumberColumn());

  rowReaderOptions.setAppendRowNumberColumn(true);
  RowReaderOptions rowReaderOptionsSecondCopy{rowReaderOptions};
  ASSERT_EQ(true, rowReaderOptionsSecondCopy.getAppendRowNumberColumn());
}
