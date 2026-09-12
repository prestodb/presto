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
package com.facebook.presto.iceberg.hadoop;

import com.facebook.presto.iceberg.AbstractTestIcebergIdentifierCharacters;
import com.facebook.presto.iceberg.IcebergQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import org.testng.annotations.Test;

import static com.facebook.presto.iceberg.CatalogType.HADOOP;

/**
 * Identifier character coverage against an Iceberg Hadoop catalog
 */
@Test(singleThreaded = true)
public class TestIcebergHadoopIdentifierCharacters
        extends AbstractTestIcebergIdentifierCharacters
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .setCatalogType(HADOOP)
                .setCreateTpchTables(false)
                .build()
                .getQueryRunner();
    }

    /**
     * This catalog leaves nested namespaces disabled, so a dot in a schema name is read as nesting
     * and refused before any character check.
     */
    @Override
    public void testDelimitedDotInASchemaName()
    {
        assertQueryFails("CREATE SCHEMA \"ident.schema\"", ".*Nested namespaces are disabled\\. Schema ident\\.schema is not valid.*");
    }

    /**
     * {@code HadoopCatalog.createNamespace} builds the namespace directory with {@code new Path},
     * where the leading {@code ident:} is read as a URI scheme. The table case is unaffected: it is
     * resolved against an already absolute namespace path.
     */
    @Override
    public void testDelimitedColonInASchemaName()
    {
        assertQueryFails("CREATE SCHEMA \"ident:schema_q\"", ".*Relative path in absolute URI: ident:schema_q.*");
    }
}
