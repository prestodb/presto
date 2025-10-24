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

import com.facebook.presto.security.DenyQueryIntegrityCheckSystemAccessControl;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.tpch.TpchQueryRunnerBuilder;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

public class TestCheckAccessPermissionsForQueryTypes
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = TpchQueryRunnerBuilder.builder()
                .setAccessControlProperties(ImmutableMap.of("access-control.name", DenyQueryIntegrityCheckSystemAccessControl.NAME)).build();

        queryRunner.loadSystemAccessControl();

        return queryRunner;
    }

    @Override
    protected QueryRunner createExpectedQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = TpchQueryRunnerBuilder.builder().build();
        return queryRunner;
    }

    @Test
    public void testCheckQueryIntegrityCalls()
    {
        assertAccessDenied("select * from orders", ".*Query integrity check failed.*");
        assertAccessDenied("analyze orders", ".*Query integrity check failed.*");
        assertAccessDenied("explain analyze select * from orders", ".*Query integrity check failed.*");
        assertAccessDenied("explain select * from orders", ".*Query integrity check failed.*");
        assertAccessDenied("explain (type validate) select * from orders", ".*Query integrity check failed.*");
        assertAccessDenied("CREATE TABLE test_empty (a BIGINT)", ".*Query integrity check failed.*");
        assertAccessDenied("use tpch.tiny", ".*Query integrity check failed.*");
    }
}
