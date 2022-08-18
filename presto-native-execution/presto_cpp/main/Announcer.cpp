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
#include "presto_cpp/external/json/json.hpp"
#include "presto_cpp/main/http/HttpClient.h"

namespace facebook::presto {
namespace {

std::string announcementBody(
    const std::string& address,
    int port,
    const std::string& nodeVersion,
    const std::string& environment,
    const std::string& nodeLocation,
    const std::vector<std::string>& connectorIds) {
  std::string id =
      boost::lexical_cast<std::string>(boost::uuids::random_generator()());

  std::ostringstream connectors;
  for (int i = 0; i < connectorIds.size(); i++) {
    if (i > 0) {
      connectors << ",";
    }
    connectors << connectorIds[i];
  }

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
           {"connectorIds", connectors.str()},
           {"http", fmt::format("http://{}:{}", address, port)}}}}}}};
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
    int port,
    std::function<folly::SocketAddress()> discoveryAddressLookup,
    const std::string& nodeVersion,
    const std::string& environment,
    const std::string& nodeId,
    const std::string& nodeLocation,
    const std::vector<std::string>& connectorIds,
    int frequencyMs)
    : discoveryAddressLookup_(std::move(discoveryAddressLookup)),
      frequencyMs_(frequencyMs),
      announcementBody_(announcementBody(
          address,
          port,
          nodeVersion,
          environment,
          nodeLocation,
          connectorIds)),
      announcementRequest_(
          announcementRequest(address, port, nodeId, announcementBody_)),
      eventBaseThread_(false /*autostart*/) {}

Announcer::~Announcer() {
  stop();
}

void Announcer::start() {
  eventBaseThread_.start("Announcer");
  stopped_ = false;
  auto* eventBase = eventBaseThread_.getEventBase();
  eventBase->runOnDestruction([this] { client_.reset(); });
  eventBase->schedule([this]() { return makeAnnouncement(); });
}

void Announcer::stop() {
  stopped_ = true;
  eventBaseThread_.stop();
}

void Announcer::makeAnnouncement() {
  // stop() calls EventBase's destructor which executed all pending callbacks;
  // make sure not to do anything if that's the case
  if (stopped_) {
    return;
  }

  try {
    auto newAddress = discoveryAddressLookup_();
    if (newAddress != address_) {
      LOG(INFO) << "Discovery service changed to " << newAddress.getAddressStr()
                << ":" << newAddress.getPort();
      std::swap(address_, newAddress);
      client_ = std::make_unique<http::HttpClient>(
          eventBaseThread_.getEventBase(),
          address_,
          std::chrono::milliseconds(10'000));
    }
  } catch (const std::exception& ex) {
    LOG(WARNING) << "Error occurred during announcement run: " << ex.what();
    scheduleNext();
    return;
  }

  client_->sendRequest(announcementRequest_, announcementBody_)
      .via(eventBaseThread_.getEventBase())
      .thenValue([](auto response) {
        auto message = response->headers();
        if (message->getStatusCode() != http::kHttpAccepted) {
          LOG(WARNING) << "Announcement failed: HTTP "
                       << message->getStatusCode() << " - "
                       << response->dumpBodyChain();
        } else {
          LOG(INFO) << "Announcement succeeded: " << message->getStatusCode();
        }
      })
      .thenError(
          folly::tag_t<std::exception>{},
          [](const std::exception& e) {
            LOG(WARNING) << "Announcement failed: " << e.what();
          })
      .thenTry([this](auto /*unused*/) { scheduleNext(); });
}

void Announcer::scheduleNext() {
  if (stopped_) {
    return;
  }
  eventBaseThread_.getEventBase()->scheduleAt(
      [this]() { return makeAnnouncement(); },
      std::chrono::steady_clock::now() +
          std::chrono::milliseconds(frequencyMs_));
}

} // namespace facebook::presto
