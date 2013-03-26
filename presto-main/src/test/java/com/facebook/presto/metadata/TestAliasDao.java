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
    private static final QualifiedTableName TABLE_1 = new QualifiedTableName("a", "b", "c");
    private static final QualifiedTableName TABLE_2 = new QualifiedTableName("d", "e", "f");
    private static final QualifiedTableName TABLE_3 = new QualifiedTableName("g", "h", "i");

    AliasDao dao;
    Handle handle;

    @BeforeMethod
    public void setup() throws Exception
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

        assertNull(dao.getAlias(TABLE_1));
    }

    @Test
    public void testAliasCreation()
    {
        createTables(dao);
        assertNull(dao.getAlias(TABLE_1));

        TableAlias tableAlias = TableAlias.createTableAlias(TABLE_1, TABLE_2);

        dao.insertAlias(tableAlias);

        TableAlias tableAlias2 = dao.getAlias(TABLE_1);

        Assert.assertEquals(tableAlias, tableAlias2);
    }

    @Test
    public void testAliasDeletion()
    {
        createTables(dao);
        assertNull(dao.getAlias(TABLE_1));

        TableAlias tableAlias = TableAlias.createTableAlias(TABLE_1, TABLE_2);

        dao.insertAlias(tableAlias);

        TableAlias tableAlias2 = dao.getAlias(TABLE_1);

        Assert.assertEquals(tableAlias, tableAlias2);

        dao.dropAlias(tableAlias);

        assertNull(dao.getAlias(TABLE_1));
    }

    @Test
    public void testDoubleDestinationFails()
    {
        createTables(dao);
        assertNull(dao.getAlias(TABLE_1));

        TableAlias tableAlias = TableAlias.createTableAlias(TABLE_1, TABLE_2);
        dao.insertAlias(tableAlias);

        try {
            tableAlias = TableAlias.createTableAlias(TABLE_3, TABLE_2);
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
        assertNull(dao.getAlias(TABLE_1));

        TableAlias tableAlias = TableAlias.createTableAlias(TABLE_1, TABLE_2);
        dao.insertAlias(tableAlias);

        tableAlias = TableAlias.createTableAlias(TABLE_1, TABLE_3);
        dao.insertAlias(tableAlias);
    }

}
