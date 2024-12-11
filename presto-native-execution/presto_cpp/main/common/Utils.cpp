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

#include "presto_cpp/main/common/Utils.h"
#include <fmt/format.h>
#include <sys/resource.h>
#include "velox/common/process/ThreadDebugInfo.h"

namespace facebook::presto::util {

protocol::DateTime toISOTimestamp(uint64_t timeMilli) {
  char buf[80];
  time_t timeSecond = timeMilli / 1000;
  tm gmtTime;
  gmtime_r(&timeSecond, &gmtTime);
  strftime(buf, sizeof buf, "%FT%T", &gmtTime);
  return fmt::format("{}.{:03d}Z", buf, timeMilli % 1000);
}

std::shared_ptr<folly::SSLContext> createSSLContext(
    const std::string& clientCertAndKeyPath,
    const std::string& ciphers) {
  try {
    auto sslContext = std::make_shared<folly::SSLContext>();
    sslContext->loadCertKeyPairFromFiles(
        clientCertAndKeyPath.c_str(), clientCertAndKeyPath.c_str());
    sslContext->setCiphersOrThrow(ciphers);
    return sslContext;
  } catch (const std::exception& ex) {
    LOG(FATAL) << fmt::format(
        "Unable to load certificate or key from {} : {}",
        clientCertAndKeyPath,
        ex.what());
  }
}

long getProcessCpuTimeNs() {
  struct rusage rusageEnd;
  getrusage(RUSAGE_SELF, &rusageEnd);

  auto tvNanos = [](struct timeval tv) {
    return tv.tv_sec * 1'000'000'000 + tv.tv_usec * 1'000;
  };

  return tvNanos(rusageEnd.ru_utime) + tvNanos(rusageEnd.ru_stime);
}

void installSignalHandler() {
#ifdef __APPLE__
  google::InstallFailureSignalHandler();
#else
  facebook::velox::process::addDefaultFatalSignalHandler();
#endif // __APPLE__
}

std::string extractMessageBody(
    const std::vector<std::unique_ptr<folly::IOBuf>>& body) {
  // TODO Avoid copy
  std::ostringstream oss;
  for (auto& buf : body) {
    oss << std::string((const char*)buf->data(), buf->length());
  }
  return oss.str();
}
} // namespace facebook::presto::util
