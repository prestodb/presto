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
package com.facebook.presto.hive.security.ranger;

import com.facebook.airlift.configuration.ConfigurationFactory;
import com.facebook.airlift.configuration.testing.ConfigAssertions;
import com.google.common.collect.ImmutableMap;
import com.google.inject.ConfigurationException;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static com.facebook.presto.hive.security.ranger.RangerBasedAccessControlConfig.RANGER_HIVE_AUDIT_PATH;
import static com.facebook.presto.hive.security.ranger.RangerBasedAccessControlConfig.RANGER_HTTP_END_POINT;
import static com.facebook.presto.hive.security.ranger.RangerBasedAccessControlConfig.RANGER_POLICY_REFRESH_PERIOD;
import static com.facebook.presto.hive.security.ranger.RangerBasedAccessControlConfig.RANGER_REST_POLICY_HIVE_SERVICE_NAME;
import static com.facebook.presto.hive.security.ranger.RangerBasedAccessControlConfig.RANGER_REST_USER_GROUPS_AUTH_PASSWORD;
import static com.facebook.presto.hive.security.ranger.RangerBasedAccessControlConfig.RANGER_REST_USER_GROUPS_AUTH_USERNAME;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestRangerBasedAccessControlConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(ConfigAssertions.recordDefaults(RangerBasedAccessControlConfig.class)
                .setRefreshPeriod(new Duration(60, TimeUnit.SECONDS))
                .setRangerHttpEndPoint(null)
                .setRangerHiveServiceName(null)
                .setBasicAuthUser(null)
                .setBasicAuthPassword(null)
                .setRangerHiveAuditPath(null));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put(RANGER_POLICY_REFRESH_PERIOD, "200s")
                .put(RANGER_HTTP_END_POINT, "http://test:6080")
                .put(RANGER_REST_POLICY_HIVE_SERVICE_NAME, "hiveServiceName")
                .put(RANGER_REST_USER_GROUPS_AUTH_USERNAME, "admin")
                .put(RANGER_REST_USER_GROUPS_AUTH_PASSWORD, "admin")
                .put(RANGER_HIVE_AUDIT_PATH, "audit_path")
                .build();

        RangerBasedAccessControlConfig expected = new RangerBasedAccessControlConfig()
                .setRefreshPeriod(new Duration(200, TimeUnit.SECONDS))
                .setRangerHttpEndPoint("http://test:6080")
                .setRangerHiveServiceName("hiveServiceName")
                .setBasicAuthUser("admin")
                .setBasicAuthPassword("admin")
                .setRangerHiveAuditPath("audit_path");
        assertFullMapping(properties, expected);
    }

    @Test
    public void testValidation()
    {
        assertThatThrownBy(() -> newInstance(ImmutableMap.of(
                RANGER_POLICY_REFRESH_PERIOD, "1us",
                RANGER_REST_POLICY_HIVE_SERVICE_NAME, "hive",
                RANGER_HTTP_END_POINT, "http://test:6080",
                RANGER_REST_USER_GROUPS_AUTH_USERNAME, "admin",
                RANGER_REST_USER_GROUPS_AUTH_PASSWORD, "admin")))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("Invalid configuration property hive.ranger.refresh-policy-period");

        assertThatThrownBy(() -> newInstance(ImmutableMap.of(
                RANGER_POLICY_REFRESH_PERIOD, "120s",
                RANGER_REST_POLICY_HIVE_SERVICE_NAME, "hive",
                RANGER_REST_USER_GROUPS_AUTH_USERNAME, "admin",
                RANGER_REST_USER_GROUPS_AUTH_PASSWORD, "admin")))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("Invalid configuration property hive.ranger.rest-endpoint: may not be null");

        assertThatThrownBy(() -> newInstance(ImmutableMap.of(
                RANGER_POLICY_REFRESH_PERIOD, "120s",
                RANGER_HTTP_END_POINT, "http://test:6080",
                RANGER_REST_USER_GROUPS_AUTH_USERNAME, "admin",
                RANGER_REST_USER_GROUPS_AUTH_PASSWORD, "admin")))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("Invalid configuration property hive.ranger.policy.hive-servicename: may not be null");
    }

    private static RangerBasedAccessControlConfig newInstance(Map<String, String> properties)
    {
        ConfigurationFactory configurationFactory = new ConfigurationFactory(properties);
        return configurationFactory.build(RangerBasedAccessControlConfig.class);
    }
}
