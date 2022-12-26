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
package com.facebook.presto.analyzer.crux;

import com.facebook.presto.Session;
import com.facebook.presto.hive.HiveQueryRunner;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.ANALYZER_TYPE;
import static org.testng.Assert.assertEquals;

public class TestCruxAnalyzer
        extends AbstractTestQueryFramework
{
    @Test(enabled = false)
    public void testSimpleQuery()
    {
        MaterializedResult materializedRows = computeActual(getCruxSession(), "select 1");
        assertEquals(materializedRows.getRowCount(), 1);
    }

    private Session getCruxSession()
    {
        return Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(ANALYZER_TYPE, "CRUX")
                .build();
    }

    @Override
    public QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = HiveQueryRunner.createQueryRunner();
        queryRunner.installPlugin(new CruxPlugin());
        return queryRunner;
    }
}
