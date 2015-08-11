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
package com.facebook.presto.raptor.metadata;

import io.airlift.dbpool.H2EmbeddedDataSource;
import io.airlift.dbpool.H2EmbeddedDataSourceConfig;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.facebook.presto.raptor.metadata.MetadataDaoUtils.createMetadataTablesWithRetry;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

@Test(singleThreaded = true)
public class TestMetadataDao
{
    private MetadataDao dao;
    private Handle handle;

    @BeforeMethod
    public void setup()
            throws Exception
    {
        handle = new DBI(new H2EmbeddedDataSource(new H2EmbeddedDataSourceConfig().setFilename("mem:"))).open();
        dao = handle.attach(MetadataDao.class);
        createMetadataTablesWithRetry(dao);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown()
    {
        handle.close();
    }

    @Test
    public void testTemporalColumn()
            throws Exception
    {
        Long columnId = 1L;
        long tableId = dao.insertTable("default", "table1");
        dao.insertColumn(tableId, columnId, "col1", 1, "bigint", null);
        Long temporalColumnId = dao.getTemporalColumnId(tableId);
        assertNull(temporalColumnId);

        dao.updateTemporalColumnId(tableId, columnId);
        temporalColumnId = dao.getTemporalColumnId(tableId);
        assertNotNull(temporalColumnId);
        assertEquals(temporalColumnId, columnId);

        long tableId2 = dao.insertTable("default", "table2");
        Long columnId2 = dao.getTemporalColumnId(tableId2);
        assertNull(columnId2);
    }
}
