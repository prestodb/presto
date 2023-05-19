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

#include "velox/exec/tests/utils/PortUtil.h"
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include "velox/common/base/Exceptions.h"

namespace facebook::velox::exec::test {

// Try binding to port. Return false if it couldn't.
bool tryBind(int port) {
  int sock = socket(PF_INET, SOCK_STREAM, 0);
  VELOX_CHECK_NE(sock, -1, "Error while creating socket: {}", errno);

  sockaddr_in addr;
  addr.sin_family = AF_INET;
  addr.sin_port = port;
  addr.sin_addr.s_addr = INADDR_ANY;

  int result = ::bind(sock, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
  return result != -1;
}

TEST(PortUtilTest, ensureAvailablePort) {
  const size_t trials = 100;
  const size_t retries = 5;

  for (size_t i = 0; i < trials; i++) {
    // Since there might be a race condition between fetching the port and
    // binding to it, try it `retries` times on each iteration.
    bool trialSucceeded = false;

    for (size_t j = 0; j < retries; j++) {
      auto port = getFreePort();
      if (tryBind(port)) {
        trialSucceeded = true;
        break;
      }
    }

    EXPECT_TRUE(trialSucceeded)
        << "Trial run #" << i << " was not able to bind to returned port after "
        << retries << " retries.";
  }
}

TEST(PortUtilTest, uniquePorts) {
  const size_t trials = 100;
  const size_t numPorts = 100;

  for (size_t i = 0; i < trials; i++) {
    auto ports = getFreePorts(numPorts);
    std::unordered_set<int> set(ports.begin(), ports.end());
    EXPECT_EQ(set.size(), numPorts)
        << "getFreePorts() returned non-unique ports.";
  }
}

} // namespace facebook::velox::exec::test
