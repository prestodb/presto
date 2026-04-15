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

#include <folly/Benchmark.h>
#include <folly/init/Init.h>
#include "presto_cpp/main/http/filters/JwtInternalAuthFilter.h"
#include "presto_cpp/main/http/tests/HttpTestBase.h"

namespace {

class HttpJwtBenchmark : public ::testing::TestWithParam<bool> {
 public:
  explicit HttpJwtBenchmark() {
    memoryPool_ = defaultMemoryManager().addLeafPool("jwtBenchmark");
    // Setup server with http
    server_ = getServer(false);
    server_->registerGet("/ping", ping);
    wrapper_ = std::make_unique<HttpServerWrapper>(std::move(server_));
  }

  std::shared_ptr<http::HttpClient> createHttpClient(
      const folly::SocketAddress& serverAddress) {
    HttpClientFactory clientFactory;
    return clientFactory.newClient(
        serverAddress, std::chrono::milliseconds(1'000), false, memoryPool_);
  }

  void TestBody(){};

  void nojwtRun() {
    folly::BenchmarkSuspender suspender;
    auto memoryPool = defaultMemoryManager().addLeafPool("benchmarkNoJwtRun");

    auto serverAddress = wrapper_->start().get();
    auto client = createHttpClient(serverAddress);
    suspender.dismiss();

    auto response = sendGet(client.get(), "/ping").get();

    suspender.rehire();
    wrapper_->stop();
    suspender.dismiss();
  }

  void jwtRun() {
    folly::BenchmarkSuspender suspender;

    const std::string kSecret{"mysecret"};
    const std::string kNodeId{"testnode"};

    std::unordered_map<std::string, std::string> systemValues{
        {std::string(SystemConfig::kMutableConfig), std::string("true")},
        {std::string(SystemConfig::kInternalCommunicationJwtEnabled),
         std::string("true")},
        {std::string(SystemConfig::kInternalCommunicationSharedSecret),
         kSecret}};
    auto rawSystemConfig =
        std::make_unique<core::MemConfigMutable>(systemValues);

    auto systemConfig = SystemConfig::instance();
    systemConfig->initialize(std::move(rawSystemConfig));

    std::unordered_map<std::string, std::string> nodeValues{
        {std::string(NodeConfig::kMutableConfig), std::string("true")},
        {std::string(NodeConfig::kNodeId), std::string(kNodeId)}};
    std::unique_ptr<Config> rawNodeConfig =
        std::make_unique<core::MemConfigMutable>(nodeValues);
    auto nodeConfig = NodeConfig::instance();
    nodeConfig->initialize(std::move(rawNodeConfig));

    std::vector<std::unique_ptr<proxygen::RequestHandlerFactory>> filters;
    filters.push_back(std::make_unique<http::filters::JWTTokenFilterFactory>());
    wrapper_->setFilters(filters);

    auto serverAddress = wrapper_->start().get();
    auto client = createHttpClient(serverAddress);
    suspender.dismiss();

    auto response = sendGet(client.get(), "/ping").get();

    suspender.rehire();
    wrapper_->stop();
    suspender.dismiss();
  }

 private:
  std::unique_ptr<http::HttpServer> server_;
  std::unique_ptr<HttpServerWrapper> wrapper_;
  std::shared_ptr<memory::MemoryPool> memoryPool_;
};

std::unique_ptr<HttpJwtBenchmark> benchmark;

template <typename Func>
void run(Func&& func, size_t iterations = 10000) {
  for (auto i = 0; i < iterations; i++) {
    func();
  }
}

BENCHMARK(nojwt) {
  run([&] { benchmark->nojwtRun(); });
}

BENCHMARK_RELATIVE(jwt) {
  run([&] { benchmark->jwtRun(); });
}
} // namespace

int main(int argc, char* argv[]) {
  folly::init(&argc, &argv);
  benchmark = std::make_unique<HttpJwtBenchmark>();
  folly::runBenchmarks();
  benchmark.reset();
  return 0;
}
