package com.facebook.presto.metadata;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestQualifiedTablePrefix
{
    @Test
    public void testCatalog()
    {
        QualifiedTablePrefix tableName = new QualifiedTablePrefix("catalog");
        assertEquals("catalog", tableName.getCatalogName());

        assertFalse(tableName.hasSchemaName());
        assertFalse(tableName.hasTableName());
    }

    @Test
    public void testSchema()
    {
        QualifiedTablePrefix tableName = new QualifiedTablePrefix("catalog", "schema");

        assertEquals("catalog", tableName.getCatalogName());
        assertTrue(tableName.hasSchemaName());

        assertEquals("schema", tableName.getSchemaName().get());
        assertFalse(tableName.hasTableName());
    }

    @Test
    public void testTable()
    {
        QualifiedTablePrefix tableName = new QualifiedTablePrefix("catalog", "schema", "table");
        assertEquals("catalog", tableName.getCatalogName());

        assertTrue(tableName.hasSchemaName());
        assertEquals("schema", tableName.getSchemaName().get());

        assertTrue(tableName.hasTableName());
        assertEquals("table", tableName.getTableName().get());
    }

    @Test
    public void testBadTable()
    {
        try {
            new QualifiedTablePrefix("catalog", null, "table");
            fail();
        }
        catch (RuntimeException e) {
            // ok
        }
    }
}
