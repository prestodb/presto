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

// Implementation of S3 filesystem and file interface.
// We provide a registration method for read and write files so the appropriate
// type of file can be constructed based on a filename. See the
// (register|generate)ReadFile and (register|generate)WriteFile functions.

#include "folly/IPAddress.h"

#include "velox/connectors/hive/storage_adapters/s3fs/S3Util.h"

namespace facebook::velox {

std::string getErrorStringFromS3Error(
    const Aws::Client::AWSError<Aws::S3::S3Errors>& error) {
  switch (error.GetErrorType()) {
    case Aws::S3::S3Errors::NO_SUCH_BUCKET:
      return "No such bucket";
    case Aws::S3::S3Errors::NO_SUCH_KEY:
      return "No such key";
    case Aws::S3::S3Errors::RESOURCE_NOT_FOUND:
      return "Resource not found";
    case Aws::S3::S3Errors::ACCESS_DENIED:
      return "Access denied";
    case Aws::S3::S3Errors::SERVICE_UNAVAILABLE:
      return "Service unavailable";
    case Aws::S3::S3Errors::NETWORK_CONNECTION:
      return "Network connection";
    default:
      return "Unknown error";
  }
}

/// The noProxyList is a comma separated list of subdomains, domains or IP
/// ranges (using CIDR). For a given hostname check if it has a matching
/// subdomain, domain or IP range in the noProxyList.
bool isHostExcludedFromProxy(
    const std::string& hostname,
    const std::string& noProxyList) {
  std::vector<std::string> noProxyListElements{};

  if (noProxyList.empty()) {
    return false;
  }

  auto hostAsIp = folly::IPAddress::tryFromString(hostname);
  folly::split(',', noProxyList, noProxyListElements);
  for (auto elem : noProxyListElements) {
    // Elem contains "/" which separates IP and subnet mask e.g. 192.168.1.0/24.
    if (elem.find("/") != std::string::npos && hostAsIp.hasValue()) {
      return hostAsIp.value().inSubnet(elem);
    }
    // Match subdomain, domain names and IP address strings.
    else if (
        elem.length() < hostname.length() && elem[0] == '.' &&
        !hostname.compare(
            hostname.length() - elem.length(), elem.length(), elem)) {
      return true;
    } else if (
        elem.length() < hostname.length() && elem[0] == '*' && elem[1] == '.' &&
        !hostname.compare(
            hostname.length() - elem.length() + 1,
            elem.length() - 1,
            elem.substr(1))) {
      return true;
    } else if (elem.length() == hostname.length() && !hostname.compare(elem)) {
      return true;
    }
  }
  return false;
}

/// Reading the various proxy related environment variables.
/// There is a lacking standard. The environment variables can be
/// defined lower case or upper case. The lower case values are checked
/// first and, if set, returned, therefore taking precendence.
/// Note, the envVar input is expected to be lower case.
namespace {
std::string readProxyEnvVar(std::string envVar) {
  auto httpProxy = getenv(envVar.c_str());
  if (httpProxy) {
    return std::string(httpProxy);
  }

  std::transform(envVar.begin(), envVar.end(), envVar.begin(), ::toupper);
  httpProxy = getenv(envVar.c_str());
  if (httpProxy) {
    return std::string(httpProxy);
  }
  return "";
};
} // namespace

std::string getHttpProxyEnvVar() {
  return readProxyEnvVar("http_proxy");
}

std::string getHttpsProxyEnvVar() {
  return readProxyEnvVar("https_proxy");
};

std::string getNoProxyEnvVar() {
  return readProxyEnvVar("no_proxy");
};

std::optional<folly::Uri> S3ProxyConfigurationBuilder::build() {
  std::string proxyUrl;
  if (useSsl_) {
    proxyUrl = getHttpsProxyEnvVar();
  } else {
    proxyUrl = getHttpProxyEnvVar();
  }

  if (proxyUrl.empty()) {
    return std::nullopt;
  }
  folly::Uri proxyUri(proxyUrl);

  /// The endpoint is usually a domain with port or an
  /// IP address with port. It is assumed that there are
  /// 2 parts separated by a colon.
  std::vector<std::string> endpointElements{};
  folly::split(':', s3Endpoint_, endpointElements);
  if (FOLLY_UNLIKELY(endpointElements.size() > 2)) {
    LOG(ERROR) << fmt::format(
        "Too many parts in S3 endpoint URI {} ", s3Endpoint_);
    return std::nullopt;
  }

  auto noProxy = getNoProxyEnvVar();
  if (isHostExcludedFromProxy(endpointElements[0], noProxy)) {
    return std::nullopt;
  }
  return proxyUri;
}

} // namespace facebook::velox
