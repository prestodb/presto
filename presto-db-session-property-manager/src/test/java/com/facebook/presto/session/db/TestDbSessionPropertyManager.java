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
package com.facebook.presto.session.db;

import com.facebook.presto.session.AbstractTestSessionPropertyManager;
import com.facebook.presto.session.SessionMatchSpec;
import com.facebook.presto.spi.session.SessionPropertyConfigurationManager;
import com.facebook.presto.testing.mysql.MySqlOptions;
import com.facebook.presto.testing.mysql.TestingMySqlServer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;

import java.util.Map;
import java.util.regex.Pattern;

import static org.testng.Assert.assertEquals;

public abstract class TestDbSessionPropertyManager
        extends AbstractTestSessionPropertyManager
{
    private static final MySqlOptions MY_SQL_OPTIONS = MySqlOptions.builder()
            .build();

    private final String driver;

    private final TestingMySqlServer mysqlServer;

    public TestDbSessionPropertyManager(String driver)
            throws Exception
    {
        this.driver = driver;
        this.mysqlServer = new TestingMySqlServer("testuser", "testpass", ImmutableList.of(), MY_SQL_OPTIONS);
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        mysqlServer.close();
    }

    @Override
    protected void assertProperties(Map<String, String> defaultProperties, SessionMatchSpec... spec)
    {
        assertProperties(defaultProperties, ImmutableMap.of(), ImmutableMap.of(), spec);
    }

    @Override
    protected void assertProperties(Map<String, String> defaultProperties, Map<String, String> overrideProperties, SessionMatchSpec... specs)
    {
        assertProperties(defaultProperties, overrideProperties, ImmutableMap.of(), specs);
    }

    @Override
    protected void assertProperties(Map<String, String> defaultProperties, Map<String, String> overrideProperties, Map<String, Map<String, String>> catalogProperties, SessionMatchSpec... specs)
    {
        DbSessionPropertyManagerConfig config = new DbSessionPropertyManagerConfig()
                .setConfigDbUrl(overrideJdbcUrl(mysqlServer.getJdbcUrl("session") + "&createDatabaseIfNotExist=true"));

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
                spec.getSessionProperties().forEach((key, value) -> dao.insertSessionProperty(finalId, key, value, null));
                spec.getCatalogSessionProperties().forEach((catalog, property) -> property.forEach((key, value) -> dao.insertSessionProperty(finalId, key, value, catalog)));
                id++;
            }
            dbSpecsProvider.refresh();
            SessionPropertyConfigurationManager.SystemSessionPropertyConfiguration propertyConfiguration = manager.getSystemSessionProperties(CONTEXT);
            assertEquals(propertyConfiguration.systemPropertyDefaults, defaultProperties);
            assertEquals(propertyConfiguration.systemPropertyOverrides, overrideProperties);
            assertEquals(manager.getCatalogSessionProperties(CONTEXT), catalogProperties);
        }
        finally {
            dao.dropSessionPropertiesTable();
            dao.dropSessionClientTagsTable();
            dao.dropSessionSpecsTable();
            dbSpecsProvider.destroy();
        }
    }

    protected String overrideJdbcUrl(String url)
    {
        return url;
    }
}
