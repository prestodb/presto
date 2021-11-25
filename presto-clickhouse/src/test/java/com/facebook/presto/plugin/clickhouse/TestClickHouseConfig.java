package com.facebook.presto.plugin.clickhouse;

import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

import com.facebook.airlift.configuration.testing.ConfigAssertions;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import java.util.Map;
import org.testng.annotations.Test;

public class TestClickHouseConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(ClickHouseConfig.class)
                .setConnectionUrl(null)
                .setConnectionUser(null)
                .setConnectionPassword(null)
                .setUserCredentialName(null)
                .setPasswordCredentialName(null)
                .setCaseInsensitiveNameMatching(false)
                .setCaseInsensitiveNameMatchingCacheTtl(new Duration(1, MINUTES))
                .setMapStringAsVarchar(false));
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
                .put("clickhouse.map-string-as-varchar", "true")
                .build();

        ClickHouseConfig expected = new ClickHouseConfig()
                .setConnectionUrl("jdbc:h2:mem:config")
                .setConnectionUser("user")
                .setConnectionPassword("password")
                .setUserCredentialName("foo")
                .setPasswordCredentialName("bar")
                .setCaseInsensitiveNameMatching(true)
                .setCaseInsensitiveNameMatchingCacheTtl(new Duration(1, SECONDS))
                .setMapStringAsVarchar(true);

        ConfigAssertions.assertFullMapping(properties, expected);
    }
}
