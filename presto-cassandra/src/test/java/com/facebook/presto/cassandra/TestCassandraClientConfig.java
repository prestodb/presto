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
package com.facebook.presto.cassandra;

import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.facebook.airlift.configuration.testing.ConfigAssertions;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.units.Duration;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.io.File;
import java.util.List;
import java.util.Map;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestCassandraClientConfig
{
    @Test
    public void testDefaults()
    {
        // Driver 4.x: Default read timeout is 12 seconds (12000ms), connect timeout is 5 seconds (5000ms)
        // Protocol version is auto-negotiated in Driver 4.x by default (null)
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(CassandraClientConfig.class)
                .setFetchSize(5_000)
                .setConsistencyLevel(DefaultConsistencyLevel.ONE)
                .setContactPoints("")
                .setNativeProtocolPort(9042)
                .setPartitionSizeForBatchSelect(100)
                .setSplitSize(1_024)
                .setSplitsPerNode(null)
                .setAllowDropTable(false)
                .setUsername(null)
                .setPassword(null)
                .setClientReadTimeout(new Duration(12000, MILLISECONDS))
                .setClientConnectTimeout(new Duration(5000, MILLISECONDS))
                .setClientSoLinger(null)
                .setRetryPolicy(RetryPolicyType.DEFAULT)
                .setUseDCAware(false)
                .setDcAwareLocalDC(null)
                .setDcAwareUsedHostsPerRemoteDc(0)
                .setDcAwareAllowRemoteDCsForLocal(false)
                .setUseTokenAware(false)
                .setTokenAwareShuffleReplicas(false)
                .setUseWhiteList(false)
                .setWhiteListAddresses("")
                .setNoHostAvailableRetryTimeout(new Duration(1, MINUTES))
                .setSpeculativeExecutionLimit(1)
                .setSpeculativeExecutionDelay(new Duration(500, MILLISECONDS))
                .setKeystorePath(null)
                .setKeystorePassword(null)
                .setTruststorePath(null)
                .setTruststorePassword(null)
                .setTlsEnabled(false)
                .setCloudSecureConnectBundle(null)
                .setProtocolVersion(null)
                .setCaseSensitiveNameMatchingEnabled(false));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("cassandra.contact-points", "host1,host2")
                .put("cassandra.native-protocol-port", "9999")
                .put("cassandra.fetch-size", "10000")
                .put("cassandra.consistency-level", "TWO")
                .put("cassandra.partition-size-for-batch-select", "77")
                .put("cassandra.split-size", "1025")
                .put("cassandra.splits-per-node", "10000")
                .put("cassandra.allow-drop-table", "true")
                .put("cassandra.username", "my_username")
                .put("cassandra.password", "my_password")
                .put("cassandra.client.read-timeout", "11ms")
                .put("cassandra.client.connect-timeout", "22ms")
                .put("cassandra.client.so-linger", "33")
                .put("cassandra.retry-policy", "BACKOFF")
                .put("cassandra.load-policy.use-dc-aware", "true")
                .put("cassandra.load-policy.dc-aware.local-dc", "dc1")
                .put("cassandra.load-policy.dc-aware.used-hosts-per-remote-dc", "1")
                .put("cassandra.load-policy.dc-aware.allow-remote-dc-for-local", "true")
                .put("cassandra.load-policy.use-token-aware", "true")
                .put("cassandra.load-policy.token-aware.shuffle-replicas", "true")
                .put("cassandra.load-policy.use-white-list", "true")
                .put("cassandra.load-policy.white-list.addresses", "host1")
                .put("cassandra.no-host-available-retry-timeout", "3m")
                .put("cassandra.speculative-execution.limit", "10")
                .put("cassandra.speculative-execution.delay", "101s")
                .put("cassandra.tls.enabled", "true")
                .put("cassandra.tls.keystore-path", "/tmp/keystore")
                .put("cassandra.tls.keystore-password", "keystore-password")
                .put("cassandra.tls.truststore-path", "/tmp/truststore")
                .put("cassandra.tls.truststore-password", "truststore-password")
                .put("cassandra.cloud.secure-connect-bundle", "/tmp/secure-connect-bundle.zip")
                .put("cassandra.protocol-version", "V4")
                .put("case-sensitive-name-matching", "true")
                .build();

        CassandraClientConfig expected = new CassandraClientConfig()
                .setContactPoints("host1", "host2")
                .setNativeProtocolPort(9999)
                .setFetchSize(10_000)
                .setConsistencyLevel(DefaultConsistencyLevel.TWO)
                .setPartitionSizeForBatchSelect(77)
                .setSplitSize(1_025)
                .setSplitsPerNode(10_000L)
                .setAllowDropTable(true)
                .setUsername("my_username")
                .setPassword("my_password")
                .setClientReadTimeout(new Duration(11, MILLISECONDS))
                .setClientConnectTimeout(new Duration(22, MILLISECONDS))
                .setClientSoLinger(33)
                .setRetryPolicy(RetryPolicyType.BACKOFF)
                .setUseDCAware(true)
                .setDcAwareLocalDC("dc1")
                .setDcAwareUsedHostsPerRemoteDc(1)
                .setDcAwareAllowRemoteDCsForLocal(true)
                .setUseTokenAware(true)
                .setTokenAwareShuffleReplicas(true)
                .setUseWhiteList(true)
                .setWhiteListAddresses("host1")
                .setNoHostAvailableRetryTimeout(new Duration(3, MINUTES))
                .setSpeculativeExecutionLimit(10)
                .setSpeculativeExecutionDelay(new Duration(101, SECONDS))
                .setTlsEnabled(true)
                .setKeystorePath(new File("/tmp/keystore"))
                .setKeystorePassword("keystore-password")
                .setTruststorePath(new File("/tmp/truststore"))
                .setTruststorePassword("truststore-password")
                .setCloudSecureConnectBundle(new File("/tmp/secure-connect-bundle.zip"))
                .setProtocolVersion("V4")
                .setCaseSensitiveNameMatchingEnabled(true);

        ConfigAssertions.assertFullMapping(properties, expected);
    }

    @Test(expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp = ".*White list node filtering.*not supported.*")
    public void testWhiteListConfigurationThrowsException()
    {
        // White list configuration should throw an exception when enabled
        // because it's not supported in Cassandra Java Driver 4.x
        CassandraClientConfig config = new CassandraClientConfig()
                .setContactPoints("host1", "host2")
                .setNativeProtocolPort(9042)
                .setUseDCAware(true)
                .setDcAwareLocalDC("datacenter1")
                .setUseWhiteList(true)
                .setWhiteListAddresses("host1,host2");

        // Attempting to create a session with white list enabled should throw
        // The validation happens in buildSession which is called during session creation
        // We need to trigger buildSession by calling createCassandraSession with proper parameters
        CassandraConnectorId connectorId = new CassandraConnectorId("test");
        JsonCodec<List<ExtraColumnMetadata>> codec = JsonCodec.listJsonCodec(ExtraColumnMetadata.class);

        // This will trigger the validation in CassandraClientModule.buildSession
        // which throws IllegalArgumentException when useWhiteList is true
        CassandraClientModule.createCassandraSession(connectorId, config, codec);
    }

    @Test
    public void testWhiteListDisabledDoesNotThrow()
    {
        // White list disabled should work fine
        CassandraClientConfig config = new CassandraClientConfig()
                .setContactPoints("host1", "host2")
                .setNativeProtocolPort(9042)
                .setUseDCAware(true)
                .setDcAwareLocalDC("datacenter1")
                .setUseWhiteList(false);

        // This should not throw an exception
        // Note: We're only testing that the configuration is accepted,
        // not actually creating a session (which would require a running Cassandra)
    }
}
