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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.tests.tpch.TpchQueryRunnerBuilder;
import com.facebook.presto.transaction.TransactionInfo;
import com.facebook.presto.transaction.TransactionManager;
import org.testng.annotations.Test;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertTrue;

public class TestTransactionManagerIntegration
{
    private static final Logger log = Logger.get(TestTransactionManagerIntegration.class);

    @Test
    public void testTransactionMetadataCleanup()
            throws Exception
    {
        try (DistributedQueryRunner queryRunner = TpchQueryRunnerBuilder.builder().build()) {
            log.info("Start running testTransactionMetadataCleanup");
            TransactionManager transactionManager = queryRunner.getTransactionManager();
            // test valid queries
            queryRunner.execute("select count(*) from nation");
            queryRunner.execute("select count(*) from lineitem");
            assertTrue(waitForTransactionMetadataCleanup(transactionManager),
                    "Remaining transaction ids: " + transactionManager.getAllTransactionInfos().stream()
                            .map(TransactionInfo::getTransactionId)
                            .collect(toImmutableList()));
            // test invalid queries with different types of semantic error
            assertThatThrownBy(() -> queryRunner.execute("SELECT non_existing_column FROM non_existing_table"))
                    .isInstanceOf(RuntimeException.class);
            assertThatThrownBy(() -> queryRunner.execute("select * from unnest('wrong_type')"))
                    .isInstanceOf(RuntimeException.class);
            assertThatThrownBy(() -> queryRunner.execute("select regionkey from nation group by regionkey having 'wrong_condition'"))
                    .isInstanceOf(RuntimeException.class);

            assertTrue(waitForTransactionMetadataCleanup(transactionManager),
                    "Remaining transaction ids: " + transactionManager.getAllTransactionInfos().stream()
                            .map(TransactionInfo::getTransactionId)
                            .collect(toImmutableList()));
            log.info("Complete test testTransactionMetadataCleanup");
        }
    }

    private boolean waitForTransactionMetadataCleanup(TransactionManager transactionManager)
            throws InterruptedException
    {
        long timeout = 5000;
        long retryInterval = 10;
        while (timeout > 0) {
            if (transactionManager.getAllTransactionInfos().isEmpty()) {
                return true;
            }
            MILLISECONDS.sleep(retryInterval);
            timeout -= retryInterval;
        }
        return false;
    }
}
