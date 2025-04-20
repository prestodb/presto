
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

 #include <gtest/gtest.h>

 #include "presto_cpp/main/common/Utils.h"

 namespace facebook::presto::test {

 using namespace ::testing;

 TEST(UtilsTest, extractMessageBody) {
   std::vector<std::unique_ptr<folly::IOBuf>> body;
   body.push_back(folly::IOBuf::copyBuffer("body1"));
   body.push_back(folly::IOBuf::copyBuffer("body2"));
   body.push_back(folly::IOBuf::copyBuffer("body3"));
   auto iobuf = folly::IOBuf::copyBuffer("body4");
   iobuf->appendToChain(folly::IOBuf::copyBuffer("body5"));
   body.push_back(std::move(iobuf));
   auto messageBody = util::extractMessageBody(body);
   EXPECT_EQ(messageBody, "body1body2body3body4body5");
 }
 } // namespace facebook::presto::test
