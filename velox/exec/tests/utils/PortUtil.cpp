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
#include <netinet/in.h>
#include <sys/socket.h>
#include <memory>
#include "velox/common/base/Exceptions.h"

namespace facebook::velox::exec::test {
namespace {

void getFreePortsImpl(int numPorts, int* ports) {
  const std::unique_ptr<int[]> sockets(new int[numPorts]);
  for (int i = 0; i < numPorts; i++) {
    int sock = socket(PF_INET, SOCK_STREAM, 0);
    VELOX_CHECK_NE(sock, -1, "Error while creating socket: {}", errno);
    sockets[i] = sock;

    sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_port = 0;
    addr.sin_addr.s_addr = INADDR_ANY;

    socklen_t len = sizeof(addr);
    int result = ::bind(sock, reinterpret_cast<sockaddr*>(&addr), len);
    VELOX_CHECK_NE(result, -1, "Error while binding socket: {}", errno);

    result = getsockname(sock, reinterpret_cast<sockaddr*>(&addr), &len);
    VELOX_CHECK_NE(result, -1, "Error on getsockname: {}", errno);

    ports[i] = ntohs(addr.sin_port);
  }

  for (int i = 0; i < numPorts; i++) {
    close(sockets[i]);
  }
}

} // namespace

std::vector<int> getFreePorts(std::size_t numPorts) {
  std::vector<int> ports;
  ports.resize(numPorts);
  getFreePortsImpl(numPorts, &ports[0]);
  return ports;
}

int getFreePort() {
  int port;
  getFreePortsImpl(1, &port);
  return port;
}

} // namespace facebook::velox::exec::test
