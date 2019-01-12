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
package io.prestosql.plugin.resourcegroups.db;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.json.JsonCodec;
import io.prestosql.spi.resourcegroups.ResourceGroupId;
import io.prestosql.spi.resourcegroups.SelectionContext;
import io.prestosql.spi.resourcegroups.SelectionCriteria;
import io.prestosql.spi.session.ResourceEstimates;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.prestosql.spi.resourcegroups.QueryType.DELETE;
import static io.prestosql.spi.resourcegroups.QueryType.INSERT;
import static io.prestosql.spi.resourcegroups.QueryType.SELECT;
import static org.testng.Assert.assertEquals;

public class TestDbSourceExactMatchSelector
{
    private static final JsonCodec<ResourceGroupId> CODEC = JsonCodec.jsonCodec(ResourceGroupId.class);
    private static final ResourceEstimates EMPTY_RESOURCE_ESTIMATES = new ResourceEstimates(Optional.empty(), Optional.empty(), Optional.empty());
    private H2ResourceGroupsDao dao;

    @BeforeClass
    public void setup()
    {
        DbResourceGroupConfig config = new DbResourceGroupConfig().setConfigDbUrl("jdbc:h2:mem:test_db-exact-match-selector" + System.nanoTime());
        dao = new H2DaoProvider(config).get();
        dao.createExactMatchSelectorsTable();
    }

    @Test
    public void testMatch()
    {
        ResourceGroupId resourceGroupId1 = new ResourceGroupId(ImmutableList.of("global", "test", "user", "insert"));
        ResourceGroupId resourceGroupId2 = new ResourceGroupId(ImmutableList.of("global", "test", "user", "select"));
        dao.insertExactMatchSelector("test", "@test@test_pipeline", INSERT.name(), CODEC.toJson(resourceGroupId1));
        dao.insertExactMatchSelector("test", "@test@test_pipeline", SELECT.name(), CODEC.toJson(resourceGroupId2));

        DbSourceExactMatchSelector selector = new DbSourceExactMatchSelector("test", dao);

        assertEquals(
                selector.match(new SelectionCriteria(true, "testuser", Optional.of("@test@test_pipeline"), ImmutableSet.of("tag"), EMPTY_RESOURCE_ESTIMATES, Optional.empty())),
                Optional.empty());
        assertEquals(
                selector.match(new SelectionCriteria(true, "testuser", Optional.of("@test@test_pipeline"), ImmutableSet.of("tag"), EMPTY_RESOURCE_ESTIMATES, Optional.of(INSERT.name()))).map(SelectionContext::getResourceGroupId),
                Optional.of(resourceGroupId1));
        assertEquals(
                selector.match(new SelectionCriteria(true, "testuser", Optional.of("@test@test_pipeline"), ImmutableSet.of("tag"), EMPTY_RESOURCE_ESTIMATES, Optional.of(SELECT.name()))).map(SelectionContext::getResourceGroupId),
                Optional.of(resourceGroupId2));
        assertEquals(
                selector.match(new SelectionCriteria(true, "testuser", Optional.of("@test@test_pipeline"), ImmutableSet.of("tag"), EMPTY_RESOURCE_ESTIMATES, Optional.of(DELETE.name()))),
                Optional.empty());

        assertEquals(
                selector.match(new SelectionCriteria(true, "testuser", Optional.of("@test@test_new"), ImmutableSet.of(), EMPTY_RESOURCE_ESTIMATES, Optional.of(INSERT.name()))),
                Optional.empty());
    }
}
