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
        QualifiedTablePrefix tableName = QualifiedTablePrefix.builder("catalog").build();
        assertEquals("catalog", tableName.getCatalogName());

        assertFalse(tableName.hasSchemaName());
        assertFalse(tableName.hasTableName());
    }

    @Test
    public void testSchema()
    {
        QualifiedTablePrefix tableName = QualifiedTablePrefix.builder("catalog").schemaName("schema").build();

        assertEquals("catalog", tableName.getCatalogName());
        assertTrue(tableName.hasSchemaName());

        assertEquals("schema", tableName.getSchemaName().get());
        assertFalse(tableName.hasTableName());
    }

    @Test
    public void testTable()
    {
        QualifiedTablePrefix tableName = QualifiedTablePrefix.builder("catalog").schemaName("schema").tableName("table").build();
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
            QualifiedTablePrefix.builder("catalog").tableName("table").build();
            fail();
        }
        catch (IllegalStateException e) {
            // ok
        }
    }

    @Test
    public void testNullOk()
    {
        QualifiedTablePrefix tableName = QualifiedTablePrefix.builder("catalog").schemaName(null).tableName(null).build();
        assertEquals("catalog", tableName.getCatalogName());

        assertFalse(tableName.hasSchemaName());
        assertFalse(tableName.hasTableName());
    }
}
