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
package com.facebook.presto.nativetests;

import com.facebook.presto.testing.QueryRunner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import static com.facebook.presto.nativetests.NativeTestsUtils.createNativeQueryRunner;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.javaHiveQueryRunnerBuilder;
import static org.testng.Assert.assertEquals;

public class TestQueryRetryOnDifferentCluster
        extends BasePlanCheckerTest
{
    protected static final String RETRY_QUERY =
            "SELECT array_agg(a) OVER(ORDER BY a ASC NULLS FIRST GROUPS BETWEEN 1 PRECEDING AND 2 FOLLOWING) " +
                    "FROM (VALUES 3, 3, 3, 2, 2, 1, null, null) T(a)";
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createNativeQueryRunner(storageFormat,
                sidecarEnabled,
                ImmutableMap.of(
                        "per-query-retry-limit", "2",
                        "retry.cross-cluster-error-codes", "NOT_SUPPORTED"));
    }

    @Override
    protected QueryRunner createExpectedQueryRunner()
            throws Exception
    {
        return javaHiveQueryRunnerBuilder()
                .setAddStorageFormatToPath(true)
                .setStorageFormat(storageFormat)
                .build();
    }

    // This query is redirected to the native cluster because failures won't be caught during an EXPLAIN (TYPE VALIDATE) call.
    // It fails on the native cluster with a "NOT_SUPPORTED" error code.
    // Since retry on the backup Java cluster is enabled, the query will be redirected there from the native coordinator.
    // The query completes successfully on the Java cluster, and the client returns success.
    @Test
    public void testRetryQueryOnBackupCluster()
            throws Exception
    {
        if (!sidecarEnabled) {
            return;
        }

        long initialNativeRedirectRequestCount = planCheckerRouterPluginPrestoClient.getNativeClusterRedirectRequests().getTotalCount();

        try (Connection connection = createConnection(httpServerUri);
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(RETRY_QUERY)) {
            ImmutableList.Builder<String> rows = ImmutableList.builder();
            while (resultSet.next()) {
                rows.add(String.valueOf(resultSet.getObject(1)));
            }

            assertEquals(rows.build().size(), 8, "Expected exactly 8 rows");
            assertEquals(rows.build(), ImmutableList.of(
                    "[null, null, 1, 2, 2]",
                    "[null, null, 1, 2, 2]",
                    "[null, null, 1, 2, 2, 3, 3, 3]",
                    "[1, 2, 2, 3, 3, 3]",
                    "[1, 2, 2, 3, 3, 3]",
                    "[2, 2, 3, 3, 3]",
                    "[2, 2, 3, 3, 3]",
                    "[2, 2, 3, 3, 3]"));

            // Ensure query redirected to Native cluster from the plan checker initially
            assertEquals(planCheckerRouterPluginPrestoClient.getNativeClusterRedirectRequests().getTotalCount(),
                    initialNativeRedirectRequestCount + 1);
        }
    }
}
