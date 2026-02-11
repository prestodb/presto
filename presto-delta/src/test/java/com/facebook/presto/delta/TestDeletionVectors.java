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
package com.facebook.presto.delta;

import com.facebook.presto.Session;
import com.facebook.presto.testing.MaterializedResult;
import org.testng.annotations.Test;

import static com.facebook.presto.delta.DeltaSessionProperties.DELETION_VECTORS_ENABLED;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;

/**
 * Integration tests for reading Delta tables with deletion vectors.
 */
public class TestDeletionVectors
        extends AbstractDeltaDistributedQueryTestBase
{
    @Test
    public void testReadTableWithDeletionVectorDisabled()
    {
        // When deletion vectors are disabled, all rows including deleted ones should be returned
        Session session = Session.builder(getSession())
                .setCatalogSessionProperty(DELTA_CATALOG, DELETION_VECTORS_ENABLED, "false")
                .build();

        String query = format("SELECT id, name FROM \"%s\".\"%s\" ORDER BY id", PATH_SCHEMA,
                goldenTablePathWithPrefix(DELTA_V3, "test_basic_dv"));

        MaterializedResult result = computeActual(session, query);
        assertEquals(result.getRowCount(), 5, "Should return all 5 rows when deletion vectors are disabled");

        assertEquals(result.getMaterializedRows().get(0).getField(0), 1);
        assertEquals(result.getMaterializedRows().get(1).getField(0), 2);
        assertEquals(result.getMaterializedRows().get(2).getField(0), 3);
        assertEquals(result.getMaterializedRows().get(3).getField(0), 4);
        assertEquals(result.getMaterializedRows().get(4).getField(0), 5);
    }

    @Test
    public void testReadTableWithDeletionVectorEnabled()
    {
        // When deletion vectors are enabled, row with id=3 should be filtered out
        Session session = Session.builder(getSession())
                .setCatalogSessionProperty(DELTA_CATALOG, DELETION_VECTORS_ENABLED, "true")
                .build();

        String query = format("SELECT id, name, value, created_date FROM \"%s\".\"%s\" ORDER BY id",
                PATH_SCHEMA, goldenTablePathWithPrefix(DELTA_V3, "test_basic_dv"));

        MaterializedResult result = computeActual(session, query);
        assertEquals(result.getRowCount(), 4, "Should return 4 rows when deletion vectors are enabled (row 3 deleted)");

        // Verify row 3 is not present
        assertEquals(result.getMaterializedRows().get(0).getField(0), 1);
        assertEquals(result.getMaterializedRows().get(0).getField(1), "Alice");

        assertEquals(result.getMaterializedRows().get(1).getField(0), 2);
        assertEquals(result.getMaterializedRows().get(1).getField(1), "Bob");

        assertEquals(result.getMaterializedRows().get(2).getField(0), 4);
        assertEquals(result.getMaterializedRows().get(2).getField(1), "Diana");

        assertEquals(result.getMaterializedRows().get(3).getField(0), 5);
        assertEquals(result.getMaterializedRows().get(3).getField(1), "Eve");
    }

    @Test
    public void testCountWithDeletionVectorEnabled()
    {
        Session session = Session.builder(getSession())
                .setCatalogSessionProperty(DELTA_CATALOG, DELETION_VECTORS_ENABLED, "true")
                .build();

        String query = format("SELECT count(*) FROM \"%s\".\"%s\"",
                PATH_SCHEMA, goldenTablePathWithPrefix(DELTA_V3, "test_basic_dv"));

        assertQuery(session, query, "SELECT 4");
    }

    @Test
    public void testFilterWithDeletionVectorEnabled()
    {
        Session session = Session.builder(getSession())
                .setCatalogSessionProperty(DELTA_CATALOG, DELETION_VECTORS_ENABLED, "true")
                .build();

        // Query for row 3 which should be deleted
        String query = format("SELECT id, name FROM \"%s\".\"%s\" WHERE id = 3",
                PATH_SCHEMA, goldenTablePathWithPrefix(DELTA_V3, "test_basic_dv"));

        MaterializedResult result = computeActual(session, query);
        assertEquals(result.getRowCount(), 0, "Row with id=3 should be deleted");
    }

    @Test
    public void testAggregationWithDeletionVectorEnabled()
    {
        Session session = Session.builder(getSession())
                .setCatalogSessionProperty(DELTA_CATALOG, DELETION_VECTORS_ENABLED, "true")
                .build();

        // Sum of values should exclude row 3 (value = 150.0)
        // Expected: 100.5 + 200.75 + 250.5 + 300.0 = 851.75
        String query = format("SELECT sum(value) FROM \"%s\".\"%s\"",
                PATH_SCHEMA, goldenTablePathWithPrefix(DELTA_V3, "test_basic_dv"));

        assertQuery(session, query, "SELECT 851.75");
    }
}
