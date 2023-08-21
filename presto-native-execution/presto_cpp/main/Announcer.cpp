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
#include "Announcer.h"

#include <boost/lexical_cast.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <folly/futures/Retrying.h>
#include <velox/common/memory/Memory.h>
#include "presto_cpp/external/json/json.hpp"

namespace facebook::presto {
namespace {

std::string announcementBody(
    const std::string& address,
    bool useHttps,
    int port,
    const std::string& nodeVersion,
    const std::string& environment,
    const std::string& nodeLocation,
    const std::vector<std::string>& connectorIds) {
  std::string id =
      boost::lexical_cast<std::string>(boost::uuids::random_generator()());

  const auto uriScheme = useHttps ? "https" : "http";

  nlohmann::json body = {
      {"environment", environment},
      {"pool", "general"},
      {"location", nodeLocation},
      {"services",
       {{{"id", id},
         {"type", "presto"},
         {"properties",
          {{"node_version", nodeVersion},
           {"coordinator", false},
           {"connectorIds", folly::join(',', connectorIds)},
           {uriScheme,
            fmt::format("{}://{}:{}", uriScheme, address, port)}}}}}}};
  return body.dump();
}

proxygen::HTTPMessage announcementRequest(
    const std::string& address,
    int port,
    const std::string& nodeId,
    const std::string& body) {
  proxygen::HTTPMessage request;
  request.setMethod(proxygen::HTTPMethod::PUT);
  request.setURL(fmt::format("/v1/announcement/{}", nodeId));
  request.getHeaders().set(
      proxygen::HTTP_HEADER_HOST, fmt::format("{}:{}", address, port));
  request.getHeaders().set(
      proxygen::HTTP_HEADER_CONTENT_TYPE, "application/json");
  request.getHeaders().set(
      proxygen::HTTP_HEADER_CONTENT_LENGTH, std::to_string(body.size()));
  return request;
}
} // namespace

Announcer::Announcer(
    const std::string& address,
    bool useHttps,
    int port,
    const std::shared_ptr<CoordinatorDiscoverer>& coordinatorDiscoverer,
    const std::string& nodeVersion,
    const std::string& environment,
    const std::string& nodeId,
    const std::string& nodeLocation,
    const std::vector<std::string>& connectorIds,
    const uint64_t maxFrequencyMs,
    const std::string& clientCertAndKeyPath,
    const std::string& ciphers)
    : PeriodicServiceInventoryManager(
          address,
          port,
          coordinatorDiscoverer,
          clientCertAndKeyPath,
          ciphers,
          "Announcement",
          maxFrequencyMs),
      announcementBody_(announcementBody(
          address,
          useHttps,
          port,
          nodeVersion,
          environment,
          nodeLocation,
          connectorIds)),
      announcementRequest_(
          announcementRequest(address, port, nodeId, announcementBody_)) {}

std::tuple<proxygen::HTTPMessage, std::string> Announcer::httpRequest() {
  return {announcementRequest_, announcementBody_};
}

} // namespace facebook::presto
