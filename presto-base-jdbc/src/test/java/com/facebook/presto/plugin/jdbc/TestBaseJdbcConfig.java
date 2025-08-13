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
package com.facebook.presto.plugin.jdbc;

import com.facebook.airlift.configuration.testing.ConfigAssertions;
import com.facebook.airlift.units.Duration;
import com.google.common.collect.ImmutableMap;
import com.google.inject.ConfigurationException;
import org.testng.annotations.Test;

import java.util.Map;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.expectThrows;

public class TestBaseJdbcConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(BaseJdbcConfig.class)
                .setConnectionUrl(null)
                .setConnectionUser(null)
                .setConnectionPassword(null)
                .setUserCredentialName(null)
                .setPasswordCredentialName(null)
                .setCaseInsensitiveNameMatching(false)
                .setCaseInsensitiveNameMatchingCacheTtl(new Duration(1, MINUTES))
                .setlistSchemasIgnoredSchemas("information_schema")
                .setCaseSensitiveNameMatching(false));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("connection-url", "jdbc:h2:mem:config")
                .put("connection-user", "user")
                .put("connection-password", "password")
                .put("user-credential-name", "foo")
                .put("password-credential-name", "bar")
                .put("case-insensitive-name-matching", "true")
                .put("case-insensitive-name-matching.cache-ttl", "1s")
                .put("list-schemas-ignored-schemas", "test,test2")
                .put("case-sensitive-name-matching", "true")
                .build();

        BaseJdbcConfig expected = new BaseJdbcConfig()
                .setConnectionUrl("jdbc:h2:mem:config")
                .setConnectionUser("user")
                .setConnectionPassword("password")
                .setUserCredentialName("foo")
                .setPasswordCredentialName("bar")
                .setCaseInsensitiveNameMatching(true)
                .setlistSchemasIgnoredSchemas("test,test2")
                .setCaseInsensitiveNameMatchingCacheTtl(new Duration(1, SECONDS))
                .setCaseSensitiveNameMatching(true);

        ConfigAssertions.assertFullMapping(properties, expected);
    }

    @Test
    public void testValidConfigValidation()
    {
        BaseJdbcConfig config = new BaseJdbcConfig();
        config.setConnectionUrl("jdbc:mysql://localhost:3306/test");

        // Should not throw any exception
        config.validateConfig();

        assertEquals(config.getConnectionUrl(), "jdbc:mysql://localhost:3306/test");
    }

    @Test
    public void testNullConnectionUrlValidation()
    {
        BaseJdbcConfig config = new BaseJdbcConfig();
        // connectionUrl is null by default

        ConfigurationException exception = expectThrows(
                ConfigurationException.class,
                config::validateConfig);
        assertEquals(exception.getErrorMessages().iterator().next().getMessage(),
                "connection-url is required but was not provided");
    }

    @Test
    public void testMutuallyExclusiveNameMatchingOptions()
    {
        BaseJdbcConfig config = new BaseJdbcConfig();
        config.setConnectionUrl("jdbc:mysql://localhost:3306/test");
        config.setCaseInsensitiveNameMatching(true);
        config.setCaseSensitiveNameMatching(true);

        ConfigurationException exception = expectThrows(
                ConfigurationException.class,
                config::validateConfig);
        assertEquals(exception.getErrorMessages().iterator().next().getMessage(),
                "Only one of 'case-insensitive-name-matching=true' or 'case-sensitive-name-matching=true' can be set. " +
                "These options are mutually exclusive.");
    }

    @Test
    public void testCaseInsensitiveNameMatchingOnly()
    {
        BaseJdbcConfig config = new BaseJdbcConfig();
        config.setConnectionUrl("jdbc:mysql://localhost:3306/test");
        config.setCaseInsensitiveNameMatching(true);
        config.setCaseSensitiveNameMatching(false);

        // Should not throw any exception
        config.validateConfig();
    }

    @Test
    public void testCaseSensitiveNameMatchingOnly()
    {
        BaseJdbcConfig config = new BaseJdbcConfig();
        config.setConnectionUrl("jdbc:mysql://localhost:3306/test");
        config.setCaseInsensitiveNameMatching(false);
        config.setCaseSensitiveNameMatching(true);

        // Should not throw any exception
        config.validateConfig();
    }
}
