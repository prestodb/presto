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
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.UUID;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createBucketedCustomer;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createBucketedLineitemAndOrders;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createCustomer;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createEmptyTable;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createLineitem;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createNation;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createOrders;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createOrdersEx;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createPart;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createPartitionedNation;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createPrestoBenchTables;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createRegion;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createSupplier;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;

public abstract class AbstractTestScaledWriter
        extends AbstractTestQueryFramework
{
    @Override
    protected void createTables()
    {
        QueryRunner queryRunner = (QueryRunner) getExpectedQueryRunner();
        createLineitem(queryRunner);
        createCustomer(queryRunner);
        createOrders(queryRunner);
        createOrdersEx(queryRunner);
        createNation(queryRunner);
        createPartitionedNation(queryRunner);
        createSupplier(queryRunner);
        createBucketedCustomer(queryRunner);
        createPart(queryRunner);
        createRegion(queryRunner);
        createEmptyTable(queryRunner);
        createBucketedLineitemAndOrders(queryRunner);
        createPrestoBenchTables(queryRunner);
    }

    @Test
    public void testScaleWriters()
    {
        Session session = buildSessionForTableWrite();
        String tmpTableName = generateRandomTableName();
        getQueryRunner().execute(session, String.format(
                "CREATE TABLE %s AS SELECT * FROM tpchstandard.tiny.orders", tmpTableName));
        assertEquals(computeActual("SELECT count(DISTINCT \"$path\") FROM " + tmpTableName).getOnlyValue(), 1L);
        dropTableIfExists(tmpTableName);

        tmpTableName = generateRandomTableName();
        getQueryRunner().execute(session, String.format(
                "CREATE TABLE %s AS SELECT * FROM tpchstandard.sf100.orders where o_orderdate > Date('1997-01-10')", tmpTableName));
        long files = (long) computeActual("SELECT count(DISTINCT \"$path\") FROM " + tmpTableName).getOnlyValue();
        long workers = (long) computeScalar("SELECT count(*) FROM system.runtime.nodes");
        assertThat(files).isBetween(2L, workers);
        dropTableIfExists(tmpTableName);
    }

    private void dropTableIfExists(String tableName)
    {
        computeExpected(String.format("DROP TABLE IF EXISTS %s", tableName), ImmutableList.of(BIGINT));
    }

    private String generateRandomTableName()
    {
        String tableName = "tmp_presto_" + UUID.randomUUID().toString().replace("-", "");
        // Clean up if the temporary named table already exists.
        dropTableIfExists(tableName);
        return tableName;
    }

    private Session buildSessionForTableWrite()
    {
        // TODO: enable this after column stats collection is enabled.
        return Session.builder(getSession())
                .setSystemProperty("scale_writers", "true")
                .setSystemProperty("task_writer_count", "1")
                .setCatalogSessionProperty("hive", "collect_column_statistics_on_write", "false")
                .setCatalogSessionProperty("hive", "optimized_partition_update_serialization_enabled", "false")
                .build();
    }
}
