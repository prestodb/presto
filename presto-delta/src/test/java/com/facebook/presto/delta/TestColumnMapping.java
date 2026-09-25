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
import com.facebook.presto.testing.MaterializedRow;
import org.testng.annotations.Test;

import java.time.LocalDate;

import static java.lang.String.format;
import static org.testng.Assert.assertEquals;

public class TestColumnMapping
        extends AbstractDeltaDistributedQueryTestBase
{
    @Test
    public void testColumnMappingSchema()
    {
        Session session = Session.builder(getSession()).build();
        String query = format("SHOW COLUMNS FROM \"%s\".\"%s\"", PATH_SCHEMA,
                goldenTablePathWithPrefix(DELTA_V3, "cm_name"));
        MaterializedResult result = computeActual(session, query);
        assertEquals(result.getMaterializedRows().size(), 5);
        assertEquals(result.getMaterializedRows().get(0).getField(0), "id");
        assertEquals(result.getMaterializedRows().get(1).getField(0), "given_name");
        assertEquals(result.getMaterializedRows().get(2).getField(0), "family_name");
        assertEquals(result.getMaterializedRows().get(3).getField(0), "email");
        assertEquals(result.getMaterializedRows().get(4).getField(0), "signup_date");
    }
    @Test
    public void testColumnMappingByNameRenamedColumns()
    {
        Session session = Session.builder(getSession()).build();
        String query = format("SELECT * FROM \"%s\".\"%s\" ORDER BY id", PATH_SCHEMA,
                goldenTablePathWithPrefix(DELTA_V3, "cm_name"));
        MaterializedResult result = computeActual(session, query);
        assertEquals(result.getMaterializedRows().size(), 5);
        MaterializedRow row1 = new MaterializedRow(5, 1, "Alice", "Smith", "alice.smith@example.com",
                LocalDate.of(2025, 1, 15));
        assertEquals(result.getMaterializedRows().get(0), row1);
        MaterializedRow row5 = new MaterializedRow(5, 5, "Eva", "Martinez", "eva.martinez@example.com",
                LocalDate.of(2026, 1, 20));
        assertEquals(result.getMaterializedRows().get(4), row5);
    }

    @Test
    public void testColumnMappingByIdRenamedColumns()
    {
        Session session = Session.builder(getSession()).build();
        String query = format("SELECT * FROM \"%s\".\"%s\" ORDER BY id", PATH_SCHEMA,
                goldenTablePathWithPrefix(DELTA_V3, "cm_id"));
        MaterializedResult result = computeActual(session, query);
        assertEquals(result.getMaterializedRows().size(), 5);
        MaterializedRow row1 = new MaterializedRow(5, 1, "Alice", "Smith", "alice.smith@example.com",
                LocalDate.of(2025, 1, 15));
        assertEquals(result.getMaterializedRows().get(0), row1);
        MaterializedRow row5 = new MaterializedRow(5, 5, "Eva", "Martinez", "eva.martinez@example.com",
                LocalDate.of(2026, 1, 20));
        assertEquals(result.getMaterializedRows().get(4), row5);
    }

    @Test
    public void testColumnMappingRenameWithSpaceAndSpecialCharactersSchema()
    {
        Session session = Session.builder(getSession()).build();
        String query = format("SHOW COLUMNS FROM \"%s\".\"%s\"", PATH_SCHEMA,
                goldenTablePathWithPrefix(DELTA_V3, "cm_sp_char"));
        MaterializedResult result = computeActual(session, query);
        assertEquals(result.getMaterializedRows().size(), 6);
        assertEquals(result.getMaterializedRows().get(0).getField(0), "id");
        assertEquals(result.getMaterializedRows().get(1).getField(0), "first name");
        assertEquals(result.getMaterializedRows().get(2).getField(0), "last@name");
        assertEquals(result.getMaterializedRows().get(3).getField(0), "e-mail (address)");
        assertEquals(result.getMaterializedRows().get(4).getField(0), "phone");
        assertEquals(result.getMaterializedRows().get(5).getField(0), "sign up #date");
    }

    @Test
    public void testColumnMappingRenameWithSpaceAndSpecialCharacters()
    {
        Session session = Session.builder(getSession()).build();
        String query = format("SELECT * FROM \"%s\".\"%s\" ORDER BY id", PATH_SCHEMA,
                goldenTablePathWithPrefix(DELTA_V3, "cm_sp_char"));
        MaterializedResult result = computeActual(session, query);
        assertEquals(result.getMaterializedRows().size(), 5);
        MaterializedRow row1 = new MaterializedRow(6, 1, "Alice", "Smith", "alice.smith@example.com",
                "555-0101", LocalDate.of(2025, 1, 15));
        assertEquals(result.getMaterializedRows().get(0), row1);
        MaterializedRow row5 = new MaterializedRow(6, 5, "Eva", "Martinez", "eva.martinez@example.com",
                "555-0105", LocalDate.of(2026, 1, 20));
        assertEquals(result.getMaterializedRows().get(4), row5);
    }

    @Test
    public void testColumnMappingDroppedColumnsSchema()
    {
        Session session = Session.builder(getSession()).build();
        String query = format("SHOW COLUMNS FROM \"%s\".\"%s\"", PATH_SCHEMA,
                goldenTablePathWithPrefix(DELTA_V3, "cm_drop"));
        MaterializedResult result = computeActual(session, query);
        assertEquals(result.getMaterializedRows().size(), 4);
        assertEquals(result.getMaterializedRows().get(0).getField(0), "id");
        assertEquals(result.getMaterializedRows().get(1).getField(0), "first_name");
        assertEquals(result.getMaterializedRows().get(2).getField(0), "last_name");
        assertEquals(result.getMaterializedRows().get(3).getField(0), "signup_date");
    }

    @Test
    public void testColumnMappingDroppedColumns()
    {
        Session session = Session.builder(getSession()).build();
        String query = format("SELECT * FROM \"%s\".\"%s\" ORDER BY id", PATH_SCHEMA,
                goldenTablePathWithPrefix(DELTA_V3, "cm_drop"));
        MaterializedResult result = computeActual(session, query);
        assertEquals(result.getMaterializedRows().size(), 5);
        MaterializedRow row1 = new MaterializedRow(5, 1, "Alice", "Smith",
                LocalDate.of(2025, 1, 15));
        assertEquals(result.getMaterializedRows().get(0), row1);
        MaterializedRow row5 = new MaterializedRow(5, 5, "Eva", "Martinez",
                LocalDate.of(2026, 1, 20));
        assertEquals(result.getMaterializedRows().get(4), row5);
    }
}
