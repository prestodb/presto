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
package com.facebook.presto.iceberg.nessie;

import com.facebook.presto.iceberg.AbstractTestIcebergIdentifierCharacters;
import com.facebook.presto.iceberg.IcebergQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.testing.containers.NessieContainer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.iceberg.CatalogType.NESSIE;
import static com.facebook.presto.iceberg.nessie.NessieTestUtil.nessieConnectorProperties;

/**
 * Identifier character coverage against an Iceberg Nessie catalog
 */
@Test(singleThreaded = true)
public class TestIcebergNessieIdentifierCharacters
        extends AbstractTestIcebergIdentifierCharacters
{
    private NessieContainer nessieContainer;

    @BeforeClass
    @Override
    public void init()
            throws Exception
    {
        nessieContainer = NessieContainer.builder().build();
        nessieContainer.start();
        super.init();
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        if (nessieContainer != null) {
            nessieContainer.stop();
        }
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .setCatalogType(NESSIE)
                .setCreateTpchTables(false)
                .setExtraConnectorProperties(nessieConnectorProperties(nessieContainer.getRestApiUri()))
                .build()
                .getQueryRunner();
    }

    /**
     * This catalog leaves nested namespaces disabled, so a dot in a schema name is read as nesting
     * and refused before any character check. The only divergence here: unlike a filesystem-backed
     * catalog, Nessie keeps namespaces in its own content model, so it takes a colon in a name.
     */
    @Override
    public void testDelimitedDotInASchemaName()
    {
        assertQueryFails("CREATE SCHEMA \"ident.schema\"", ".*Nested namespaces are disabled\\. Schema ident\\.schema is not valid.*");
    }
}
