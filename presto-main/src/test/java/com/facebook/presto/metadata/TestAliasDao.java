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
package com.facebook.presto.metadata;

import io.airlift.dbpool.H2EmbeddedDataSource;
import io.airlift.dbpool.H2EmbeddedDataSourceConfig;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.exceptions.UnableToExecuteStatementException;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.sql.DataSource;

import static com.facebook.presto.metadata.AliasDao.Utils.createTables;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.fail;

public class TestAliasDao
{
    private static final String TABLE_1_CONNECTOR_ID = "apple_connector";
    private static final String TABLE_1_SCHEMA_NAME = "apple_schema";
    private static final String TABLE_1_TABLE_NAME = "apple_table";
    private static final String TABLE_2_CONNECTOR_ID = "banana_connector";
    private static final String TABLE_2_SCHEMA_NAME = "banana_schema";
    private static final String TABLE_2_TABLE_NAME = "banana_table";
    private static final String TABLE_3_CONNECTOR_ID = "cherry_connector";
    private static final String TABLE_3_SCHEMA_NAME = "cherry_schema";
    private static final String TABLE_3_TABLE_NAME = "cherry_table";

    AliasDao dao;
    Handle handle;

    @BeforeMethod
    public void setup()
            throws Exception
    {
        H2EmbeddedDataSourceConfig dataSourceConfig = new H2EmbeddedDataSourceConfig().setFilename("mem:");
        DataSource dataSource = new H2EmbeddedDataSource(dataSourceConfig);
        DBI h2Dbi = new DBI(dataSource);
        handle = h2Dbi.open();
        dao = handle.attach(AliasDao.class);
    }

    @AfterMethod
    public void teardown()
    {
        handle.close();
    }

    @Test
    public void testTableCreation()
    {
        createTables(dao);

        assertNull(dao.getAlias(TABLE_1_CONNECTOR_ID, TABLE_1_SCHEMA_NAME, TABLE_1_TABLE_NAME));
    }

    @Test
    public void testAliasCreation()
    {
        createTables(dao);
        assertNull(dao.getAlias(TABLE_1_CONNECTOR_ID, TABLE_1_SCHEMA_NAME, TABLE_1_TABLE_NAME));

        TableAlias tableAlias = new TableAlias(TABLE_1_CONNECTOR_ID,
                TABLE_1_SCHEMA_NAME,
                TABLE_1_TABLE_NAME,
                TABLE_2_CONNECTOR_ID,
                TABLE_2_SCHEMA_NAME,
                TABLE_2_TABLE_NAME);

        dao.insertAlias(tableAlias);

        TableAlias tableAlias2 = dao.getAlias(TABLE_1_CONNECTOR_ID, TABLE_1_SCHEMA_NAME, TABLE_1_TABLE_NAME);

        Assert.assertEquals(tableAlias, tableAlias2);
    }

    @Test
    public void testAliasDeletion()
    {
        createTables(dao);
        assertNull(dao.getAlias(TABLE_1_CONNECTOR_ID, TABLE_1_SCHEMA_NAME, TABLE_1_TABLE_NAME));

        TableAlias tableAlias = new TableAlias(TABLE_1_CONNECTOR_ID,
                TABLE_1_SCHEMA_NAME,
                TABLE_1_TABLE_NAME,
                TABLE_2_CONNECTOR_ID,
                TABLE_2_SCHEMA_NAME,
                TABLE_2_TABLE_NAME);

        dao.insertAlias(tableAlias);

        TableAlias tableAlias2 = dao.getAlias(TABLE_1_CONNECTOR_ID, TABLE_1_SCHEMA_NAME, TABLE_1_TABLE_NAME);

        Assert.assertEquals(tableAlias, tableAlias2);

        dao.dropAlias(tableAlias);

        assertNull(dao.getAlias(TABLE_1_CONNECTOR_ID, TABLE_1_SCHEMA_NAME, TABLE_1_TABLE_NAME));
    }

    @Test
    public void testDoubleDestinationFails()
    {
        createTables(dao);
        assertNull(dao.getAlias(TABLE_1_CONNECTOR_ID, TABLE_1_SCHEMA_NAME, TABLE_1_TABLE_NAME));

        TableAlias tableAlias = new TableAlias(TABLE_1_CONNECTOR_ID,
                TABLE_1_SCHEMA_NAME,
                TABLE_1_TABLE_NAME,
                TABLE_2_CONNECTOR_ID,
                TABLE_2_SCHEMA_NAME,
                TABLE_2_TABLE_NAME);
        dao.insertAlias(tableAlias);

        try {
            tableAlias = new TableAlias(TABLE_3_CONNECTOR_ID,
                    TABLE_3_SCHEMA_NAME,
                    TABLE_3_TABLE_NAME,
                    TABLE_2_CONNECTOR_ID,
                    TABLE_2_SCHEMA_NAME,
                    TABLE_2_TABLE_NAME);
            dao.insertAlias(tableAlias);
            fail("Could insert table twice!");
        }
        catch (UnableToExecuteStatementException e) {
            // ok
        }
    }

    @Test
    public void testDoubleSourceOk()
    {
        createTables(dao);
        assertNull(dao.getAlias(TABLE_1_CONNECTOR_ID, TABLE_1_SCHEMA_NAME, TABLE_1_TABLE_NAME));

        TableAlias tableAlias = new TableAlias(TABLE_1_CONNECTOR_ID,
                TABLE_1_SCHEMA_NAME,
                TABLE_1_TABLE_NAME,
                TABLE_2_CONNECTOR_ID,
                TABLE_2_SCHEMA_NAME,
                TABLE_2_TABLE_NAME);
        dao.insertAlias(tableAlias);

        tableAlias = new TableAlias(TABLE_1_CONNECTOR_ID,
                TABLE_1_SCHEMA_NAME,
                TABLE_1_TABLE_NAME,
                TABLE_3_CONNECTOR_ID,
                TABLE_3_SCHEMA_NAME,
                TABLE_3_TABLE_NAME);
        dao.insertAlias(tableAlias);
    }
}
