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
#include <folly/compression/Compression.h>
#include <folly/io/Cursor.h>
#include <folly/io/IOBuf.h>
#include <sys/resource.h>
#include "velox/common/base/Exceptions.h"
#include "velox/common/process/ThreadDebugInfo.h"

namespace facebook::presto::util {

DateTime toISOTimestamp(uint64_t timeMilli) {
  char buf[80];
  time_t timeSecond = timeMilli / 1000;
  tm gmtTime;
  gmtime_r(&timeSecond, &gmtTime);
  strftime(buf, sizeof buf, "%FT%T", &gmtTime);
  return fmt::format("{}.{:03d}Z", buf, timeMilli % 1000);
}

std::shared_ptr<folly::SSLContext> createSSLContext(
    const std::string& clientCertAndKeyPath,
    const std::string& ciphers,
    bool http2Enabled) {
  try {
    auto sslContext = std::make_shared<folly::SSLContext>();
    sslContext->loadCertKeyPairFromFiles(
        clientCertAndKeyPath.c_str(), clientCertAndKeyPath.c_str());
    sslContext->setCiphersOrThrow(ciphers);
    if (http2Enabled) {
      sslContext->setAdvertisedNextProtocols({"h2", "http/1.1"});
    } else {
      sslContext->setAdvertisedNextProtocols({"http/1.1"});
    }
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
  std::string ret;
  size_t bodySize = 0;
  for (const auto& buf : body) {
    bodySize += buf->computeChainDataLength();
  }
  ret.resize(bodySize);

  size_t offset = 0;
  for (const auto& buf : body) {
    folly::io::Cursor cursor(buf.get());
    size_t chainLength = buf->computeChainDataLength();
    cursor.pull(ret.data() + offset, chainLength);
    offset += chainLength;
  }
  return ret;
}

std::string decompressMessageBody(
    const std::vector<std::unique_ptr<folly::IOBuf>>& body,
    const std::string& contentEncoding) {
  try {
    // Combine all IOBufs into a single chain
    std::unique_ptr<folly::IOBuf> combined;
    for (const auto& buf : body) {
      if (!combined) {
        combined = buf->clone();
      } else {
        combined->appendToChain(buf->clone());
      }
    }

    // Determine compression codec type; Support only ZSTD for now
    folly::compression::CodecType codecType;
    if (contentEncoding == "zstd") {
      codecType = folly::compression::CodecType::ZSTD;
    } else {
      VELOX_USER_FAIL("Unsupported Content-Encoding: {}", contentEncoding);
    }

    // Decompress the data
    auto codec = folly::compression::getCodec(
        codecType); // getCodec never return nullptr
    auto decompressed = codec->uncompress(combined.get());

    size_t decompressedSize = decompressed->computeChainDataLength();

    // Convert decompressed IOBuf to string
    std::string ret;
    ret.resize(decompressedSize);
    folly::io::Cursor cursor(decompressed.get());
    cursor.pull(ret.data(), decompressedSize);

    return ret;
  } catch (const std::exception& e) {
    VELOX_USER_FAIL(
        "Failed to decompress request body with {}: {}",
        contentEncoding,
        e.what());
  }
}

const std::vector<std::string> getFunctionNameParts(
    const std::string& registeredFunction) {
  std::vector<std::string> parts;
  folly::split('.', registeredFunction, parts, true);
  VELOX_USER_CHECK(
      parts.size() == 3,
      fmt::format("Prefix missing for function {}", registeredFunction));
  return parts;
}

std::string boolToString(bool value) {
  return value ? "true" : "false";
}
} // namespace facebook::presto::util
