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

#include "velox/connectors/hive/storage_adapters/s3fs/S3Util.h"

#include "gtest/gtest.h"

namespace facebook::velox {

// TODO: Each prefix should be implemented as its own filesystem.
TEST(S3UtilTest, isS3File) {
  EXPECT_FALSE(isS3File("ss3://"));
  EXPECT_FALSE(isS3File("s3:/"));
  EXPECT_FALSE(isS3File("oss:"));
  EXPECT_FALSE(isS3File("cos:"));
  EXPECT_FALSE(isS3File("cosn:"));
  EXPECT_FALSE(isS3File("S3A://bucket/some/file.txt"));
  EXPECT_FALSE(isS3File("OSS://other-bucket/some/file.txt"));
  EXPECT_FALSE(isS3File("COS://other-bucket/some/file.txt"));
  EXPECT_FALSE(isS3File("COSN://other-bucket/some/file.txt"));
  EXPECT_FALSE(isS3File("s3::/bucket"));
  EXPECT_FALSE(isS3File("s3:/bucket"));
  EXPECT_FALSE(isS3File("file://bucket"));
  EXPECT_TRUE(isS3File("s3://bucket/file.txt"));
  EXPECT_TRUE(isS3File("s3n://bucket/file.txt"));
}

TEST(S3UtilTest, isS3AwsFile) {
  EXPECT_FALSE(isS3AwsFile("s3:"));
  EXPECT_FALSE(isS3AwsFile("s3::/bucket"));
  EXPECT_FALSE(isS3AwsFile("s3:/bucket"));
  EXPECT_TRUE(isS3AwsFile("s3://bucket/file.txt"));
}

TEST(S3UtilTest, isS3aFile) {
  EXPECT_FALSE(isS3aFile("s3a:"));
  EXPECT_FALSE(isS3aFile("s3a::/bucket"));
  EXPECT_FALSE(isS3aFile("s3a:/bucket"));
  EXPECT_FALSE(isS3aFile("S3A://bucket-name/file.txt"));
  EXPECT_TRUE(isS3aFile("s3a://bucket/file.txt"));
}

TEST(S3UtilTest, isS3nFile) {
  EXPECT_FALSE(isS3nFile("s3n:"));
  EXPECT_FALSE(isS3nFile("s3n::/bucket"));
  EXPECT_FALSE(isS3nFile("s3n:/bucket"));
  EXPECT_FALSE(isS3nFile("S3N://bucket-name/file.txt"));
  EXPECT_TRUE(isS3nFile("s3n://bucket/file.txt"));
}

TEST(S3UtilTest, isOssFile) {
  EXPECT_FALSE(isOssFile("oss:"));
  EXPECT_FALSE(isOssFile("oss::/bucket"));
  EXPECT_FALSE(isOssFile("oss:/bucket"));
  EXPECT_FALSE(isOssFile("OSS://BUCKET/sub-key/file.txt"));
  EXPECT_TRUE(isOssFile("oss://bucket/file.txt"));
}

TEST(S3UtilTest, isCosFile) {
  EXPECT_FALSE(isCosFile("cos:"));
  EXPECT_FALSE(isCosFile("cos::/bucket"));
  EXPECT_FALSE(isCosFile("cos:/bucket"));
  EXPECT_FALSE(isCosFile("COS://BUCKET/sub-key/file.txt"));
  EXPECT_TRUE(isCosFile("cos://bucket/file.txt"));
}

TEST(S3UtilTest, isCosNFile) {
  EXPECT_FALSE(isCosNFile("cosn:"));
  EXPECT_FALSE(isCosNFile("cosn::/bucket"));
  EXPECT_FALSE(isCosNFile("cosn:/bucket"));
  EXPECT_FALSE(isCosNFile("COSN://BUCKET/sub-key/file.txt"));
  EXPECT_TRUE(isCosNFile("cosn://bucket/file.txt"));
}

// TODO: Each prefix should be implemented as its own filesystem.
TEST(S3UtilTest, s3Path) {
  auto path_0 = s3Path("s3://bucket/file.txt");
  auto path_1 = s3Path("oss://bucket-name/file.txt");
  auto path_2 = s3Path("S3A://bucket-NAME/sub-PATH/my-file.txt");
  auto path_3 = s3Path("s3N://bucket-NAME/sub-PATH/my-file.txt");
  auto path_4 = s3Path("cos://bucket-name/file.txt");
  auto path_5 = s3Path("cosn://bucket-name/file.txt");
  EXPECT_EQ(path_0, "bucket/file.txt");
  EXPECT_EQ(path_1, "bucket-name/file.txt");
  EXPECT_NE(path_2, "bucket-NAME/sub-PATH/my-file.txt");
  EXPECT_NE(path_3, "bucket-NAME/sub-PATH/my-file.txt");
  EXPECT_EQ(path_4, "bucket-name/file.txt");
  EXPECT_EQ(path_5, "bucket-name/file.txt");
}

TEST(S3UtilTest, bucketAndKeyFromS3Path) {
  std::string bucket, key;
  auto path = "bucket/file.txt";
  getBucketAndKeyFromS3Path(path, bucket, key);
  EXPECT_EQ(bucket, "bucket");
  EXPECT_EQ(key, "file.txt");
}

TEST(S3UtilTest, isDomainExcludedFromProxy) {
  auto hostname = "test.foobar.com";

  std::vector<std::pair<std::string, bool>> tests = {
      {"localhost,.foobar.com", true},
      {"localhost,.,foobar.com,.com", true},
      {"localhost,test.foobar.com", true},
      {"localhost,foobar.com,*.com", true},
      {"localhost,*.foobar.com", true},
      {"localhost", false},
      {"localhost,foobar.com", false},
      {"", false},
  };

  for (auto pair : tests) {
    EXPECT_EQ(isHostExcludedFromProxy(hostname, pair.first), pair.second);
  }
}

TEST(S3UtilTest, isIpExcludedFromProxy) {
  auto hostname = "127.0.0.1";

  std::vector<std::pair<std::string, bool>> tests = {
      {"localhost,127.0.0.1,.foobar.com", true},
      {"localhost,foobar.com,.1,.com", true},
      {"localhost,test.foobar.com", false},
      {"localhost,foobar.com,*.1,*.com", true},
      {"localhost", false},
      {"localhost,127.1.0.1", false},
      {"", false},
  };

  for (auto pair : tests) {
    EXPECT_EQ(isHostExcludedFromProxy(hostname, pair.first), pair.second)
        << pair.first;
  }
}

class S3UtilProxyTest : public ::testing::TestWithParam<bool> {};

TEST_P(S3UtilProxyTest, proxyBuilderBadEndpoint) {
  auto s3Endpoint = "http://127.0.0.1:8888";
  auto useSsl = GetParam();

  setenv("HTTP_PROXY", "http://127.0.0.1:12345", 1);
  setenv("HTTPS_PROXY", "http://127.0.0.1:12345", 1);
  EXPECT_FALSE(S3ProxyConfigurationBuilder(s3Endpoint)
                   .useSsl(useSsl)
                   .build()
                   .has_value());
}

TEST_P(S3UtilProxyTest, proxyBuilderNoProxy) {
  auto s3Endpoint = "127.0.0.1:8888";
  auto useSsl = GetParam();

  setenv("HTTP_PROXY", "", 1);
  setenv("HTTPS_PROXY", "", 1);
  EXPECT_FALSE(S3ProxyConfigurationBuilder(s3Endpoint)
                   .useSsl(useSsl)
                   .build()
                   .has_value());
}

TEST_P(S3UtilProxyTest, proxyBuilderSameHttpProxy) {
  auto s3Endpoint = "192.168.0.1:12345";
  auto useSsl = GetParam();

  setenv("HTTP_PROXY", "http://127.0.0.1:8888", 1);
  setenv("HTTPS_PROXY", "http://127.0.0.1:8888", 1);
  auto proxyConfig =
      S3ProxyConfigurationBuilder(s3Endpoint).useSsl(useSsl).build();
  ASSERT_TRUE(proxyConfig.has_value());
  EXPECT_EQ(proxyConfig.value().scheme(), "http");
  EXPECT_EQ(proxyConfig.value().host(), "127.0.0.1");
  EXPECT_EQ(proxyConfig.value().port(), 8888);
  EXPECT_EQ(proxyConfig.value().username(), "");
  EXPECT_EQ(proxyConfig.value().password(), "");
}

TEST_P(S3UtilProxyTest, proxyBuilderMixProxy) {
  auto s3Endpoint = "192.168.0.1:12345";
  auto useSsl = GetParam();

  const std::string httpProxy = "https://test1:testpw1@80.67.3.1:35631";
  setenv("HTTP_PROXY", httpProxy.c_str(), 1);
  EXPECT_EQ(getHttpProxyEnvVar(), httpProxy)
      << "HTTP_PROXY environment variable not set.";
  const std::string httpsProxy = "http://test2:testpw2@80.80.5.1:45631";
  setenv("HTTPS_PROXY", httpsProxy.c_str(), 1);
  EXPECT_EQ(getHttpsProxyEnvVar(), httpsProxy)
      << "HTTPS_PROXY environment variable not set.";
  auto proxyConfig =
      S3ProxyConfigurationBuilder(s3Endpoint).useSsl(useSsl).build();
  ASSERT_TRUE(proxyConfig.has_value());
  EXPECT_EQ(proxyConfig.value().scheme(), (useSsl ? "http" : "https"));
  EXPECT_EQ(proxyConfig.value().host(), (useSsl ? "80.80.5.1" : "80.67.3.1"));
  EXPECT_EQ(proxyConfig.value().port(), (useSsl ? 45631 : 35631));
  EXPECT_EQ(proxyConfig.value().username(), (useSsl ? "test2" : "test1"));
  EXPECT_EQ(proxyConfig.value().password(), (useSsl ? "testpw2" : "testpw1"));
}

TEST_P(S3UtilProxyTest, proxyBuilderMixProxyLowerCase) {
  auto s3Endpoint = "192.168.0.1:12345";
  auto useSsl = GetParam();

  const std::string lcHttpProxy = "https://lctest1:lctestpw1@80.67.3.1:35631";
  const std::string ucHttpProxy = "https://uctest1:uctestpw1@80.67.3.2:35632";
  setenv("http_proxy", lcHttpProxy.c_str(), 1);
  setenv("HTTP_PROXY", ucHttpProxy.c_str(), 1);
  // Lower case value takes precedence.
  EXPECT_EQ(getHttpProxyEnvVar(), lcHttpProxy)
      << "http_proxy environment variable not set.";
  const std::string lcHttpsProxy = "http://lctest2:lctestpw2@80.80.5.1:45631";
  const std::string ucHttpsProxy = "http://uctest2:uctestpw2@80.80.5.2:45632";
  setenv("https_proxy", lcHttpsProxy.c_str(), 1);
  setenv("HTTPS_PROXY", ucHttpsProxy.c_str(), 1);
  EXPECT_EQ(getHttpsProxyEnvVar(), lcHttpsProxy)
      << "https_proxy environment variable not set.";
  auto proxyConfig =
      S3ProxyConfigurationBuilder(s3Endpoint).useSsl(useSsl).build();
  ASSERT_TRUE(proxyConfig.has_value());
  EXPECT_EQ(proxyConfig.value().scheme(), (useSsl ? "http" : "https"));
  EXPECT_EQ(proxyConfig.value().host(), (useSsl ? "80.80.5.1" : "80.67.3.1"));
  EXPECT_EQ(proxyConfig.value().port(), (useSsl ? 45631 : 35631));
  EXPECT_EQ(proxyConfig.value().username(), (useSsl ? "lctest2" : "lctest1"));
  EXPECT_EQ(
      proxyConfig.value().password(), (useSsl ? "lctestpw2" : "lctestpw1"));
}

INSTANTIATE_TEST_SUITE_P(
    S3UtilTest,
    S3UtilProxyTest,
    ::testing::Values(true, false));

} // namespace facebook::velox
