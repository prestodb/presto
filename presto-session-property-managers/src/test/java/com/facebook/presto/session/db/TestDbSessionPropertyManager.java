package com.facebook.presto.session.db;

import com.facebook.presto.session.AbstractTestSessionPropertyManager;
import com.facebook.presto.session.SessionMatchSpec;
import com.facebook.presto.spi.session.SessionPropertyConfigurationManager;
import com.facebook.presto.testing.mysql.MySqlOptions;
import com.facebook.presto.testing.mysql.TestingMySqlServer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;

import java.util.Map;
import java.util.regex.Pattern;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;

public class TestDbSessionPropertyManager
        extends AbstractTestSessionPropertyManager
{
    private static final MySqlOptions MY_SQL_OPTIONS = MySqlOptions.builder()
            .setCommandTimeout(new Duration(90, SECONDS))
            .build();

    private final TestingMySqlServer mysqlServer;

    public TestDbSessionPropertyManager()
            throws Exception
    {
        this.mysqlServer = new TestingMySqlServer("testuser", "testpass", ImmutableList.of(), MY_SQL_OPTIONS);
    }

    @Override
    protected void assertProperties(Map<String, String> defaultProperties, SessionMatchSpec... spec)
    {
        assertProperties(defaultProperties, ImmutableMap.of(), spec);
    }

    @Override
    protected void assertProperties(Map<String, String> defaultProperties, Map<String, String> overrideProperties, SessionMatchSpec... specs)
    {
        DbSessionPropertyManagerConfig config = new DbSessionPropertyManagerConfig()
                .setConfigDbUrl(mysqlServer.getJdbcUrl("testSession"))
                .setPassword(mysqlServer.getPassword())
                .setUsername(mysqlServer.getUser());
        SessionPropertiesDaoProvider sessionPropertiesDaoProvider = new SessionPropertiesDaoProvider(config);
        SessionPropertiesDao dao = sessionPropertiesDaoProvider.get();
        RefreshingDbSpecsProvider dbSpecsProvider = new RefreshingDbSpecsProvider(config, sessionPropertiesDaoProvider.get());
        SessionPropertyConfigurationManager manager = new DbSessionPropertyManager(dbSpecsProvider);
        int id = 1;
        try {
            for (SessionMatchSpec spec : specs) {
                int finalId = id;
                dao.insertSpecRow(
                        finalId,
                        spec.getUserRegex().map(Pattern::pattern).orElse(null),
                        spec.getSourceRegex().map(Pattern::pattern).orElse(null),
                        spec.getQueryType().orElse(null),
                        spec.getResourceGroupRegex().map(Pattern::pattern).orElse(null),
                        spec.getClientInfoRegex().map(Pattern::pattern).orElse(null),
                        spec.getOverrideSessionProperties().map(val -> val ? 1 : 0).orElse(null),
                        finalId);
                spec.getClientTags().forEach(tag -> dao.insertClientTag(finalId, tag));
                spec.getSessionProperties().forEach((key, value) -> dao.insertSessionProperty(finalId, key, value));
                id++;
            }
            dbSpecsProvider.refresh();
            SessionPropertyConfigurationManager.SystemSessionPropertyConfiguration propertyConfiguration = manager.getSystemSessionProperties(CONTEXT);
            assertEquals(propertyConfiguration.systemPropertyDefaults, defaultProperties);
            assertEquals(propertyConfiguration.systemPropertyOverrides, overrideProperties);
        }
        finally {
            dao.dropSessionPropertiesTable();
            dao.dropSessionClientTagsTable();
            dao.dropSessionSpecsTable();
            dbSpecsProvider.destroy();
        }
    }
}
