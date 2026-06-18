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

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.facebook.airlift.log.Logger;
import com.facebook.airlift.units.Duration;
import com.facebook.presto.spi.PrestoException;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestCassandraClientModule
{
    @Test
    public void testBuildDriverConfigDefaults()
    {
        CassandraClientConfig config = new CassandraClientConfig()
                .setContactPoints("localhost")
                .setDcAwareLocalDC("datacenter1");
        DriverExecutionProfile profile = buildProfile(config);

        assertEquals(profile.getDuration(DefaultDriverOption.REQUEST_TIMEOUT).toMillis(), 12_000L);
        assertEquals(profile.getDuration(DefaultDriverOption.CONNECTION_CONNECT_TIMEOUT).toMillis(), 5_000L);
        assertEquals(profile.getString(DefaultDriverOption.REQUEST_CONSISTENCY), "LOCAL_ONE");
        assertEquals(profile.getInt(DefaultDriverOption.REQUEST_PAGE_SIZE), 5_000);
        assertEquals(profile.getString(DefaultDriverOption.RECONNECTION_POLICY_CLASS), "ExponentialReconnectionPolicy");
        assertEquals(profile.getDuration(DefaultDriverOption.RECONNECTION_BASE_DELAY).toMillis(), 500L);
        assertEquals(profile.getDuration(DefaultDriverOption.RECONNECTION_MAX_DELAY).toMillis(), 10_000L);
        assertEquals(profile.getString(DefaultDriverOption.RETRY_POLICY_CLASS), "DefaultRetryPolicy");
        assertEquals(profile.getString(DefaultDriverOption.LOAD_BALANCING_POLICY_CLASS), "DefaultLoadBalancingPolicy");
        // Speculative execution is left at the driver's default policy (the connector only overrides it when limit > 1).
        // The driver's bundled reference.conf always defines the policy class, so assert the default value rather than absence.
        assertEquals(profile.getString(DefaultDriverOption.SPECULATIVE_EXECUTION_POLICY_CLASS), "NoSpeculativeExecutionPolicy");
        assertFalse(profile.isDefined(DefaultDriverOption.SPECULATIVE_EXECUTION_MAX));
        assertFalse(profile.isDefined(DefaultDriverOption.SOCKET_LINGER_INTERVAL));
        assertFalse(profile.isDefined(DefaultDriverOption.PROTOCOL_VERSION));
    }

    @Test
    public void testBuildDriverConfigCustomTimeoutsAndConsistency()
    {
        CassandraClientConfig config = new CassandraClientConfig()
                .setContactPoints("localhost")
                .setDcAwareLocalDC("datacenter1")
                .setClientReadTimeout(new Duration(3_000, TimeUnit.MILLISECONDS))
                .setClientConnectTimeout(new Duration(1_000, TimeUnit.MILLISECONDS))
                .setConsistencyLevel(com.datastax.oss.driver.api.core.DefaultConsistencyLevel.QUORUM)
                .setFetchSize(10_000);
        DriverExecutionProfile profile = buildProfile(config);

        assertEquals(profile.getDuration(DefaultDriverOption.REQUEST_TIMEOUT).toMillis(), 3_000L);
        assertEquals(profile.getDuration(DefaultDriverOption.CONNECTION_CONNECT_TIMEOUT).toMillis(), 1_000L);
        assertEquals(profile.getString(DefaultDriverOption.REQUEST_CONSISTENCY), "QUORUM");
        assertEquals(profile.getInt(DefaultDriverOption.REQUEST_PAGE_SIZE), 10_000);
    }

    @Test
    public void testBuildDriverConfigWithProtocolVersion()
    {
        CassandraClientConfig config = new CassandraClientConfig()
                .setContactPoints("localhost")
                .setDcAwareLocalDC("datacenter1")
                .setProtocolVersion("V4");
        DriverExecutionProfile profile = buildProfile(config);

        assertTrue(profile.isDefined(DefaultDriverOption.PROTOCOL_VERSION));
        assertEquals(profile.getString(DefaultDriverOption.PROTOCOL_VERSION), "V4");
    }

    @Test
    public void testBuildDriverConfigWithSpeculativeExecution()
    {
        CassandraClientConfig config = new CassandraClientConfig()
                .setContactPoints("localhost")
                .setDcAwareLocalDC("datacenter1")
                .setSpeculativeExecutionLimit(3)
                .setSpeculativeExecutionDelay(new Duration(200, TimeUnit.MILLISECONDS));
        DriverExecutionProfile profile = buildProfile(config);

        assertEquals(profile.getString(DefaultDriverOption.SPECULATIVE_EXECUTION_POLICY_CLASS), "ConstantSpeculativeExecutionPolicy");
        assertEquals(profile.getInt(DefaultDriverOption.SPECULATIVE_EXECUTION_MAX), 3);
        assertEquals(profile.getDuration(DefaultDriverOption.SPECULATIVE_EXECUTION_DELAY).toMillis(), 200L);
    }

    @Test
    public void testBuildDriverConfigSpeculativeExecutionNotEnabledWhenLimitIsOne()
    {
        CassandraClientConfig config = new CassandraClientConfig()
                .setContactPoints("localhost")
                .setDcAwareLocalDC("datacenter1")
                .setSpeculativeExecutionLimit(1);
        DriverExecutionProfile profile = buildProfile(config);

        // A limit of 1 means no speculative execution, so the driver's default policy must remain in place.
        assertEquals(profile.getString(DefaultDriverOption.SPECULATIVE_EXECUTION_POLICY_CLASS), "NoSpeculativeExecutionPolicy");
        assertFalse(profile.isDefined(DefaultDriverOption.SPECULATIVE_EXECUTION_MAX));
    }

    @Test
    public void testBuildDriverConfigWithSoLinger()
    {
        CassandraClientConfig config = new CassandraClientConfig()
                .setContactPoints("localhost")
                .setDcAwareLocalDC("datacenter1")
                .setClientSoLinger(5);
        DriverExecutionProfile profile = buildProfile(config);

        assertTrue(profile.isDefined(DefaultDriverOption.SOCKET_LINGER_INTERVAL));
        assertEquals(profile.getInt(DefaultDriverOption.SOCKET_LINGER_INTERVAL), 5);
    }

    @Test
    public void testBuildDriverConfigWithDcFailover()
    {
        CassandraClientConfig config = new CassandraClientConfig()
                .setContactPoints("localhost")
                .setDcAwareLocalDC("datacenter1")
                .setDcAwareUsedHostsPerRemoteDc(2)
                .setDcAwareAllowRemoteDCsForLocal(true);
        DriverExecutionProfile profile = buildProfile(config);

        assertEquals(profile.getInt(DefaultDriverOption.LOAD_BALANCING_DC_FAILOVER_MAX_NODES_PER_REMOTE_DC), 2);
        assertTrue(profile.getBoolean(DefaultDriverOption.LOAD_BALANCING_DC_FAILOVER_ALLOW_FOR_LOCAL_CONSISTENCY_LEVELS));
    }

    @Test
    public void testBuildDriverConfigDcFailoverNotAppliedWhenZero()
    {
        CassandraClientConfig config = new CassandraClientConfig()
                .setContactPoints("localhost")
                .setDcAwareLocalDC("datacenter1")
                .setDcAwareUsedHostsPerRemoteDc(0);
        DriverExecutionProfile profile = buildProfile(config);

        // With zero remote hosts the connector writes no DC-failover options, so the driver's
        // reference.conf defaults (0 nodes, failover disabled) must remain in effect.
        assertEquals(profile.getInt(DefaultDriverOption.LOAD_BALANCING_DC_FAILOVER_MAX_NODES_PER_REMOTE_DC), 0);
        assertFalse(profile.getBoolean(DefaultDriverOption.LOAD_BALANCING_DC_FAILOVER_ALLOW_FOR_LOCAL_CONSISTENCY_LEVELS));
    }

    @Test
    public void testBuildDriverConfigWithBackoffRetryPolicy()
    {
        CassandraClientConfig config = new CassandraClientConfig()
                .setContactPoints("localhost")
                .setDcAwareLocalDC("datacenter1")
                .setRetryPolicy(RetryPolicyType.BACKOFF);
        DriverExecutionProfile profile = buildProfile(config);

        assertEquals(profile.getString(DefaultDriverOption.RETRY_POLICY_CLASS), BackoffRetryPolicy.class.getName());
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = ".*Local datacenter must be specified.*")
    public void testConfigureEndpointThrowsWhenLocalDcMissing()
    {
        CassandraClientConfig config = new CassandraClientConfig()
                .setContactPoints("localhost");
        Logger log = Logger.get(CassandraClientModule.class);
        CassandraClientModule.configureEndpoint(CqlSession.builder(), config, log);
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = ".*Local datacenter must be specified.*")
    public void testConfigureEndpointThrowsWhenLocalDcIsBlank()
    {
        CassandraClientConfig config = new CassandraClientConfig()
                .setContactPoints("localhost")
                .setDcAwareLocalDC("   ");
        Logger log = Logger.get(CassandraClientModule.class);
        CassandraClientModule.configureEndpoint(CqlSession.builder(), config, log);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = ".*empty contactPoints.*")
    public void testConfigureEndpointThrowsForEmptyContactPoints()
    {
        CassandraClientConfig config = new CassandraClientConfig();
        Logger log = Logger.get(CassandraClientModule.class);
        CassandraClientModule.configureEndpoint(CqlSession.builder(), config, log);
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = ".*Secure connect bundle not found.*")
    public void testConfigureEndpointThrowsForMissingSecureBundle()
    {
        CassandraClientConfig config = new CassandraClientConfig()
                .setSecureConnectBundle(new java.io.File("/nonexistent/path/bundle.zip"));
        Logger log = Logger.get(CassandraClientModule.class);
        CassandraClientModule.configureEndpoint(CqlSession.builder(), config, log);
    }

    private static DriverExecutionProfile buildProfile(CassandraClientConfig config)
    {
        DriverConfigLoader loader = CassandraClientModule.buildDriverConfig(config);
        return loader.getInitialConfig().getDefaultProfile();
    }
}
