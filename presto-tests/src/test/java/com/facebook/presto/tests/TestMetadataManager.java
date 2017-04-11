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
package com.facebook.presto.tests;

import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.testing.QueryRunner;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import static com.facebook.presto.tests.tpch.TpchQueryRunner.createQueryRunner;
import static org.testng.Assert.assertEquals;

/**
 * This is integration / unit test suite.
 * The reason for having it here is to ensure that we won't leak memory in MetadataManager
 * while registering catalog -> query Id mapping.
 * This mapping has to be manually cleaned when query finishes execution (Metadata#cleanupQuery method).
 */
@Test(singleThreaded = true)
public class TestMetadataManager
{
    private final QueryRunner queryRunner;
    private final MetadataManager metadataManager;

    TestMetadataManager()
            throws Exception
    {
        queryRunner = createQueryRunner();
        metadataManager = (MetadataManager) queryRunner.getMetadata();
    }

    @Test
    public void testMetadataIsClearedAfterQueryFinished()
    {
        @Language("SQL") String sql = "SELECT * FROM nation";
        queryRunner.execute(sql);

        assertEquals(metadataManager.getCatalogsByQueryId().size(), 0);
    }

    @Test
    public void testMetadataIsClearedAfterQueryFailed()
    {
        @Language("SQL") String sql = "SELECT nationkey/0 FROM nation"; // will raise division by zero exception
        try {
            queryRunner.execute(sql);
        }
        catch (Throwable t) {
            // query should fail
        }

        assertEquals(metadataManager.getCatalogsByQueryId().size(), 0);
    }
}
