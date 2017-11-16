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
package com.facebook.presto.resourceGroups.db;

import com.facebook.presto.resourceGroups.ResourceGroupNameTemplate;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.google.common.collect.ImmutableList;
import io.airlift.json.JsonCodec;
import io.airlift.units.Duration;
import org.h2.jdbc.JdbcSQLException;
import org.skife.jdbi.v2.exceptions.UnableToExecuteStatementException;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

import static io.airlift.json.JsonCodec.listJsonCodec;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestResourceGroupsDao
{
    private static final String ENVIRONMENT = "test";
    private static final JsonCodec<List<String>> LIST_STRING_CODEC = listJsonCodec(String.class);

    static H2ResourceGroupsDao setup(String prefix)
    {
        DbResourceGroupConfig config = new DbResourceGroupConfig().setConfigDbUrl("jdbc:h2:mem:test_" + prefix + System.nanoTime());
        return new H2DaoProvider(config).get();
    }

    @Test
    public void testResourceGroups()
    {
        H2ResourceGroupsDao dao = setup("resource_groups");
        dao.createResourceGroupsTable();
        Map<Long, ResourceGroupSpecBuilder> map = new HashMap<>();
        testResourceGroupInsert(dao, map);
        testResourceGroupUpdate(dao, map);
        testResourceGroupDelete(dao, map);
    }

    private static void testResourceGroupInsert(H2ResourceGroupsDao dao, Map<Long, ResourceGroupSpecBuilder> map)
    {
        dao.insertResourceGroup(1, "global", "100%", 100, 100, 100, null, null, null, null, null, null, null, null, ENVIRONMENT);
        dao.insertResourceGroup(2, "bi", "50%", 50, 50, 50, null, null, null, null, null, null, null, 1L, ENVIRONMENT);
        List<ResourceGroupSpecBuilder> records = dao.getResourceGroups(ENVIRONMENT);
        assertEquals(records.size(), 2);
        map.put(1L, new ResourceGroupSpecBuilder(1, new ResourceGroupNameTemplate("global"), "100%", 100, Optional.of(100), 100, Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), null));
        map.put(2L, new ResourceGroupSpecBuilder(2, new ResourceGroupNameTemplate("bi"), "50%", 50, Optional.of(50), 50, Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.of(1L)));
        compareResourceGroups(map, records);
    }

    private static void testResourceGroupUpdate(H2ResourceGroupsDao dao, Map<Long, ResourceGroupSpecBuilder> map)
    {
        dao.updateResourceGroup(2, "bi", "40%", 40, 30, 30, null, null, true, null, null, null, null, 1L, ENVIRONMENT);
        ResourceGroupSpecBuilder updated = new ResourceGroupSpecBuilder(2, new ResourceGroupNameTemplate("bi"), "40%", 40, Optional.of(30), 30, Optional.empty(), Optional.empty(), Optional.of(true), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.of(1L));
        map.put(2L, updated);
        compareResourceGroups(map, dao.getResourceGroups(ENVIRONMENT));
    }

    private static void testResourceGroupDelete(H2ResourceGroupsDao dao, Map<Long, ResourceGroupSpecBuilder> map)
    {
        dao.deleteResourceGroup(2);
        map.remove(2L);
        compareResourceGroups(map, dao.getResourceGroups(ENVIRONMENT));
    }

    @Test
    public void testSelectors()
    {
        H2ResourceGroupsDao dao = setup("selectors");
        dao.createResourceGroupsTable();
        dao.createSelectorsTable();
        Map<Long, SelectorRecord> map = new HashMap<>();
        testSelectorInsert(dao, map);
        testSelectorUpdate(dao, map);
        testSelectorUpdateNull(dao, map);
        testSelectorDelete(dao, map);
        testSelectorDeleteNull(dao, map);
        testSelectorMultiDelete(dao, map);
    }

    private static void testSelectorInsert(H2ResourceGroupsDao dao, Map<Long, SelectorRecord> map)
    {
        map.put(2L,
                new SelectorRecord(
                        2L,
                        Optional.of(Pattern.compile("ping_user")),
                        Optional.of(Pattern.compile(".*")),
                        Optional.empty()));
        map.put(3L,
                new SelectorRecord(
                        3L,
                        Optional.of(Pattern.compile("admin_user")),
                        Optional.of(Pattern.compile(".*")),
                        Optional.of(ImmutableList.of("tag1", "tag2"))));
        dao.insertResourceGroup(1, "admin", "100%", 100, 100, 100, null, null, null, null, null, null, null, null, ENVIRONMENT);
        dao.insertResourceGroup(2, "ping_query", "50%", 50, 50, 50, null, null, null, null, null, null, null, 1L, ENVIRONMENT);
        dao.insertResourceGroup(3, "config", "50%", 50, 50, 50, null, null, null, null, null, null, null, 1L, ENVIRONMENT);
        dao.insertSelector(2, "ping_user", ".*", null);
        dao.insertSelector(3, "admin_user", ".*", LIST_STRING_CODEC.toJson(ImmutableList.of("tag1", "tag2")));
        List<SelectorRecord> records = dao.getSelectors();
        compareSelectors(map, records);
    }

    private static void testSelectorUpdate(H2ResourceGroupsDao dao, Map<Long, SelectorRecord> map)
    {
        dao.updateSelector(2, "ping.*", "ping_source", LIST_STRING_CODEC.toJson(ImmutableList.of("tag1")), "ping_user", ".*", null);
        SelectorRecord updated = new SelectorRecord(
                2,
                Optional.of(Pattern.compile("ping.*")),
                Optional.of(Pattern.compile("ping_source")),
                Optional.of(ImmutableList.of("tag1")));
        map.put(2L, updated);
        compareSelectors(map, dao.getSelectors());
    }

    private static void testSelectorUpdateNull(H2ResourceGroupsDao dao, Map<Long, SelectorRecord> map)
    {
        SelectorRecord updated = new SelectorRecord(2, Optional.empty(), Optional.empty(), Optional.empty());
        map.put(2L, updated);
        dao.updateSelector(2, null, null, null, "ping.*", "ping_source", LIST_STRING_CODEC.toJson(ImmutableList.of("tag1")));
        compareSelectors(map, dao.getSelectors());
        updated = new SelectorRecord(
                2,
                Optional.of(Pattern.compile("ping.*")),
                Optional.of(Pattern.compile("ping_source")),
                Optional.of(ImmutableList.of("tag1", "tag2")));
        map.put(2L, updated);
        dao.updateSelector(2, "ping.*", "ping_source", LIST_STRING_CODEC.toJson(ImmutableList.of("tag1", "tag2")), null, null, null);
        compareSelectors(map, dao.getSelectors());
    }

    private static void testSelectorDelete(H2ResourceGroupsDao dao, Map<Long, SelectorRecord> map)
    {
        map.remove(2L);
        dao.deleteSelector(2, "ping.*", "ping_source", LIST_STRING_CODEC.toJson(ImmutableList.of("tag1", "tag2")));
        compareSelectors(map, dao.getSelectors());
    }

    private static void testSelectorDeleteNull(H2ResourceGroupsDao dao, Map<Long, SelectorRecord> map)
    {
        dao.updateSelector(3, null, null, null, "admin_user", ".*", LIST_STRING_CODEC.toJson(ImmutableList.of("tag1", "tag2")));
        SelectorRecord nullRegexes = new SelectorRecord(3L, Optional.empty(), Optional.empty(), Optional.empty());
        map.put(3L, nullRegexes);
        compareSelectors(map, dao.getSelectors());
        dao.deleteSelector(3, null, null, null);
        map.remove(3L);
        compareSelectors(map, dao.getSelectors());
    }

    private static void testSelectorMultiDelete(H2ResourceGroupsDao dao, Map<Long, SelectorRecord> map)
    {
        if (dao != null) {
            return;
        }

        dao.insertSelector(3, "user1", "pipeline", null);
        map.put(3L, new SelectorRecord(
                3L,
                Optional.of(Pattern.compile("user1")),
                Optional.of(Pattern.compile("pipeline")),
                Optional.empty()));
        compareSelectors(map, dao.getSelectors());
        dao.deleteSelectors(3L);
        map.remove(3L);
        compareSelectors(map, dao.getSelectors());
    }

    @Test
    public void testGlobalResourceGroupProperties()
    {
        H2ResourceGroupsDao dao = setup("global_properties");
        dao.createResourceGroupsGlobalPropertiesTable();
        dao.insertResourceGroupsGlobalProperties("cpu_quota_period", "1h");
        ResourceGroupGlobalProperties globalProperties = new ResourceGroupGlobalProperties(Optional.of(Duration.valueOf("1h")));
        ResourceGroupGlobalProperties records = dao.getResourceGroupGlobalProperties().get(0);
        assertEquals(globalProperties, records);
        try {
            dao.insertResourceGroupsGlobalProperties("invalid_property", "1h");
        }
        catch (UnableToExecuteStatementException ex) {
            assertTrue(ex.getCause() instanceof JdbcSQLException);
            assertTrue(ex.getCause().getMessage().startsWith("Check constraint violation:"));
        }
        try {
            dao.updateResourceGroupsGlobalProperties("invalid_property_name");
        }
        catch (UnableToExecuteStatementException ex) {
            assertTrue(ex.getCause() instanceof JdbcSQLException);
            assertTrue(ex.getCause().getMessage().startsWith("Check constraint violation:"));
        }
    }

    @Test
    public void testExactMatchSelector()
    {
        H2ResourceGroupsDao dao = setup("exact_match_selector");
        dao.createExactMatchSelectorsTable();

        ResourceGroupId resourceGroupId = new ResourceGroupId(ImmutableList.of("global", "test", "user"));
        JsonCodec<ResourceGroupId> codec = JsonCodec.jsonCodec(ResourceGroupId.class);
        dao.insertExactMatchSelector("test", "@test@test_pipeline", codec.toJson(resourceGroupId));

        assertEquals(dao.getExactMatchResourceGroup("test", "@test@test_pipeline"), codec.toJson(resourceGroupId));
        assertNull(dao.getExactMatchResourceGroup("test", "abc"));
        assertNull(dao.getExactMatchResourceGroup("prod", "@test@test_pipeline"));
    }

    private static void compareResourceGroups(Map<Long, ResourceGroupSpecBuilder> map, List<ResourceGroupSpecBuilder> records)
    {
        assertEquals(map.size(), records.size());
        for (ResourceGroupSpecBuilder record : records) {
            ResourceGroupSpecBuilder expected = map.get(record.getId());
            assertEquals(record.build(), expected.build());
        }
    }

    private static void compareSelectors(Map<Long, SelectorRecord> map, List<SelectorRecord> records)
    {
        assertEquals(map.size(), records.size());
        for (SelectorRecord record : records) {
            SelectorRecord expected = map.get(record.getResourceGroupId());
            assertEquals(record.getResourceGroupId(), expected.getResourceGroupId());
            assertEquals(record.getUserRegex().map(Pattern::pattern), expected.getUserRegex().map(Pattern::pattern));
            assertEquals(record.getSourceRegex().map(Pattern::pattern), expected.getSourceRegex().map(Pattern::pattern));
        }
    }
}
