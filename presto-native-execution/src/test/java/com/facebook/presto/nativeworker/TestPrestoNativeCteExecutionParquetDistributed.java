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
import com.facebook.presto.testing.QueryRunner;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.CTE_FILTER_AND_PROJECTION_PUSHDOWN_ENABLED;
import static com.facebook.presto.SystemSessionProperties.CTE_MATERIALIZATION_STRATEGY;
import static com.facebook.presto.SystemSessionProperties.PARTITIONING_PROVIDER_CATALOG;
import static com.facebook.presto.SystemSessionProperties.PUSHDOWN_SUBFIELDS_ENABLED;
import static com.facebook.presto.SystemSessionProperties.VERBOSE_OPTIMIZER_INFO_ENABLED;

@Test(groups = {"parquet"})
public class TestPrestoNativeCteExecutionParquetDistributed
        extends AbstractTestNativeCteExecution
{
    private final String storageFormat = "PARQUET";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createQueryRunner(storageFormat, false);
    }

    @Override
    protected QueryRunner createExpectedQueryRunner()
            throws Exception
    {
        return PrestoNativeQueryRunnerUtils.javaHiveQueryRunnerBuilder()
                .setStorageFormat(this.storageFormat)
                .setAddStorageFormatToPath(true)
                .build();
    }

    @Override
    protected Session getSession()
    {
        return Session.builder(super.getSession())
                .setSystemProperty(PUSHDOWN_SUBFIELDS_ENABLED, "true")
                .setSystemProperty(CTE_MATERIALIZATION_STRATEGY, "NONE")
                .build();
    }

    @Override
    protected Session getMaterializedSession()
    {
        return Session.builder(super.getSession())
                .setSystemProperty(PUSHDOWN_SUBFIELDS_ENABLED, "true")
                .setSystemProperty(VERBOSE_OPTIMIZER_INFO_ENABLED, "true")
                .setSystemProperty(PARTITIONING_PROVIDER_CATALOG, "hive")
                .setSystemProperty(CTE_MATERIALIZATION_STRATEGY, "ALL")
                .setSystemProperty(CTE_FILTER_AND_PROJECTION_PUSHDOWN_ENABLED, "true")
                .build();
    }
}
