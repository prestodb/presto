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
package com.facebook.presto.iceberg;

import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestIntegrationSmokeTest;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.iceberg.IcebergQueryRunner.createIcebergQueryRunner;

public class TestIcebergSmoke
        extends AbstractTestIntegrationSmokeTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createIcebergQueryRunner(ImmutableMap.of());
    }

    @Override
    protected boolean isParameterizedVarcharSupported()
    {
        return false;
    }

    @Test
    public void testDecimal()
    {
        // TODO
    }

    @Test
    public void testTimestamp()
    {
        // TODO
    }

    @Test
    public void testCreatePartitionedTable()
    {
        // TODO
    }

    @Test
    public void testCreatePartitionedTableAs()
    {
        // TODO
    }
}
