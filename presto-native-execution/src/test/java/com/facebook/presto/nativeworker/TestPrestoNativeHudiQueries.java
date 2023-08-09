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
package com.facebook.presto.nativeworker;

import com.facebook.presto.Session;
import com.facebook.presto.spi.security.SelectedRole;
import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.hive.HiveQueryRunner.HUDI_SCHEMA;
import static com.facebook.presto.hive.HiveQueryRunner.createSession;
import static com.facebook.presto.spi.security.SelectedRole.Type.ROLE;
import static java.lang.String.format;

public class TestPrestoNativeHudiQueries
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return PrestoNativeQueryRunnerUtils.createNativeQueryRunner(false, "PARQUET", true);
    }

    @Override
    protected ExpectedQueryRunner createExpectedQueryRunner()
            throws Exception
    {
        return PrestoNativeQueryRunnerUtils.createJavaQueryRunner("PARQUET", true);
    }

    @Test
    public void testQuery()
    {
        @Language("SQL") String sqlTemplate = "SELECT symbol, max(ts) FROM %s GROUP BY symbol HAVING symbol = 'GOOG'";
        @Language("SQL") String sqlResult = "SELECT 'GOOG', '2018-08-31 10:59:00'";
        Session session = createSession(Optional.of(new SelectedRole(ROLE, Optional.of("admin"))), HUDI_SCHEMA);
        assertQuery(session, format(sqlTemplate, "stock_ticks_cown"), sqlResult);
    }
}
