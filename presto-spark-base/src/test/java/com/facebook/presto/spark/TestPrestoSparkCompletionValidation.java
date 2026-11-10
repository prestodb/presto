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
package com.facebook.presto.spark;

import com.facebook.presto.Session;
import com.facebook.presto.execution.QueryCompletionValidator;
import com.facebook.presto.execution.StageExecutionInfo;
import com.facebook.presto.spark.execution.nativeprocess.NativeExecutionModule;
import com.facebook.presto.spark.execution.property.NativeExecutionConfigModule;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.sql.planner.PlanFragment;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.facebook.presto.spark.PrestoSparkSessionProperties.SPARK_ADAPTIVE_QUERY_EXECUTION_ENABLED;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

public class TestPrestoSparkCompletionValidation
{
    @Test(timeOut = 180000)
    public void testStaticAndAdaptiveValidateBeforeCommit()
            throws Exception
    {
        AtomicBoolean enabled = new AtomicBoolean();
        AtomicBoolean reject = new AtomicBoolean();
        AtomicInteger checks = new AtomicInteger();
        Set<String> checkedStages = ConcurrentHashMap.newKeySet();
        QueryCompletionValidator validator = new QueryCompletionValidator()
        {
            @Override
            public boolean isEnabled(Session session)
            {
                return enabled.get();
            }

            @Override
            public void validate(Session session, PlanFragment fragment, StageExecutionInfo info)
            {
                assertTrue(info.isFinal());
                checkedStages.add(fragment.getId().toString());
                checks.incrementAndGet();
                if (reject.get()) {
                    throw new PrestoException(GENERIC_USER_ERROR, "spark-completion-rejected");
                }
            }
        };
        try (PrestoSparkQueryRunner runner = new PrestoSparkQueryRunner("hive", ImmutableMap.of(),
                ImmutableMap.of(), ImmutableMap.of(), Optional.empty(), ImmutableList.of(
                        new NativeExecutionModule(), new NativeExecutionConfigModule(ImmutableMap.of(),
                                ImmutableMap.of("hive", ImmutableMap.of("connector.name", "hive"))),
                        binder -> newSetBinder(binder, QueryCompletionValidator.class).addBinding().toInstance(validator)), 4)) {
            runner.execute("CREATE SCHEMA IF NOT EXISTS hive.validation");
            for (boolean adaptive : new boolean[] {false, true}) {
                Session session = Session.builder(runner.getDefaultSession()).setSchema("validation")
                        .setSystemProperty(SPARK_ADAPTIVE_QUERY_EXECUTION_ENABLED, Boolean.toString(adaptive)).build();
                enabled.set(false);
                int before = checks.get();
                runner.execute(session, "SELECT 1");
                assertEquals(checks.get(), before);
                enabled.set(true);
                reject.set(false);
                runner.execute(session, "SELECT 1");
                assertTrue(checks.get() > before);
                String acceptedTable = "accepted_" + adaptive;
                runner.execute(session, "CREATE TABLE " + acceptedTable + " AS SELECT x FROM UNNEST(SEQUENCE(1, 10)) t(x)");
                assertTrue(runner.tableExists(session, acceptedTable));
                checkedStages.clear();
                runner.execute(session, "SELECT sum(x) FROM " + acceptedTable);
                assertTrue(checkedStages.size() > 1, "The passing distributed query must validate every stage");
                reject.set(true);
                String table = "rejected_" + adaptive;
                RuntimeException failure = expectThrows(RuntimeException.class,
                        () -> runner.execute(session, "CREATE TABLE " + table + " AS SELECT 1 x"));
                assertTrue(failure.getMessage().contains("spark-completion-rejected"));
                assertFalse(runner.tableExists(session, table));
                reject.set(false);
                Session capped = Session.builder(session)
                        .setSystemProperty("spark_max_task_infos_in_query_completed_event", "0").build();
                int beforeSkipped = checks.get();
                reject.set(true);
                String skippedTable = "skipped_" + adaptive;
                runner.execute(capped, "CREATE TABLE " + skippedTable + " AS SELECT 1 x");
                assertTrue(runner.tableExists(capped, skippedTable));
                assertEquals(checks.get(), beforeSkipped);
                reject.set(false);
            }
        }
    }
}
