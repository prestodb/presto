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

import com.facebook.presto.iceberg.IcebergDistributedSmokeTestBase;
import com.facebook.presto.iceberg.IcebergQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.testing.containers.NessieContainer;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.iceberg.CatalogType.NESSIE;
import static com.facebook.presto.iceberg.nessie.NessieTestUtil.nessieConnectorProperties;

@Test
public class TestIcebergSmokeNessie
        extends IcebergDistributedSmokeTestBase
{
    private NessieContainer nessieContainer;

    public TestIcebergSmokeNessie()
    {
        super(NESSIE);
    }

    @BeforeClass
    @Override
    public void init()
            throws Exception
    {
        nessieContainer = NessieContainer.builder().build();
        nessieContainer.start();
        super.init();
    }

    @AfterClass
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
        return IcebergQueryRunner.createIcebergQueryRunner(ImmutableMap.of(), nessieConnectorProperties(nessieContainer.getRestApiUri()));
    }
}
