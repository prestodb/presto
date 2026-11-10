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
package com.facebook.presto.iceberg.hive;

import com.facebook.presto.iceberg.AbstractTestIcebergIdentifierCharacters;
import com.facebook.presto.iceberg.IcebergQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import org.testng.annotations.Test;

import static com.facebook.presto.iceberg.CatalogType.HIVE;

/**
 * Identifier character coverage against an Iceberg Hive catalog
 */
@Test(singleThreaded = true)
public class TestIcebergHiveIdentifierCharacters
        extends AbstractTestIcebergIdentifierCharacters
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .setCatalogType(HIVE)
                .setCreateTpchTables(false)
                .build()
                .getQueryRunner();
    }

    /**
     * The only case where this catalog is stricter than the REST one. A Hive catalog derives the
     * schema's warehouse directory from its name and resolves it as a URI, and a colon there reads
     * as a scheme separator, so the schema cannot be created at all.
     */
    @Override
    public void testDelimitedColonInASchemaName()
    {
        assertQueryFails("CREATE SCHEMA \"ident:schema_q\"",
                ".*Relative path in absolute URI: ident:schema_q.*");
    }

    @Override
    public void testDelimitedColonInATableName()
    {
        assertUpdate("CREATE SCHEMA ident_colon_schema");
        try {
            assertQueryFails("CREATE TABLE ident_colon_schema.\"ident:table_q\" (id integer)", ".*Relative path in absolute URI: ident:table_q.*");
        }
        finally {
            assertQuerySucceeds("DROP SCHEMA ident_colon_schema");
        }
    }
}
