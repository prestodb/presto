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

import com.facebook.presto.Session;
import com.facebook.presto.operator.HashAggregationOperator;
import com.facebook.presto.operator.HashBuilderOperator;
import com.facebook.presto.operator.TaskMemoryReservationSummary;
import com.facebook.presto.operator.WindowOperator;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.facebook.airlift.json.JsonCodec.listJsonCodec;
import static com.facebook.presto.SystemSessionProperties.USE_MARK_DISTINCT;
import static com.facebook.presto.SystemSessionProperties.VERBOSE_EXCEEDED_MEMORY_LIMIT_ERRORS_ENABLED;
import static java.util.regex.Pattern.DOTALL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
public abstract class AbstractTestVerboseMemoryExceededErrors
        extends AbstractTestQueryFramework
{
    private static final int INVOCATION_COUNT = 1;

    @Override
    protected Session getSession()
    {
        return Session.builder(super.getSession())
                .setSystemProperty(VERBOSE_EXCEEDED_MEMORY_LIMIT_ERRORS_ENABLED, "true")
                .setSystemProperty(USE_MARK_DISTINCT, "false")
                .build();
    }

    @Test(invocationCount = INVOCATION_COUNT)
    public void testAggregation()
    {
        assertMemoryExceededDetails("" +
                        "SELECT " +
                        "   linenumber, " +
                        "   ARRAY_AGG(comment)," +
                        "   MAP_AGG(comment, comment) " +
                        "FROM lineitem " +
                        "GROUP BY linenumber",
                HashAggregationOperator.class.getSimpleName(),
                Optional.empty());
        assertMemoryExceededDetails("" +
                        "SELECT " +
                        "   linenumber, " +
                        "   ARRAY_AGG(DISTINCT comment)," +
                        "   MAP_AGG(comment, comment) " +
                        "FROM lineitem " +
                        "GROUP BY linenumber",
                HashAggregationOperator.class.getSimpleName(),
                Optional.of("DISTINCT;"));
        assertMemoryExceededDetails("" +
                        "SELECT " +
                        "   linenumber, " +
                        "   ARRAY_AGG(comment ORDER BY comment)," +
                        "   MAP_AGG(comment, comment) " +
                        "FROM lineitem " +
                        "GROUP BY linenumber",
                HashAggregationOperator.class.getSimpleName(),
                Optional.of("ORDER_BY;"));
    }

    @Test(invocationCount = INVOCATION_COUNT)
    public void testJoin()
    {
        assertMemoryExceededDetails("" +
                        "SELECT " +
                        "   * " +
                        "FROM lineitem l1 " +
                        "INNER JOIN lineitem l2 " +
                        "ON l1.linenumber = l2.linenumber " +
                        "WHERE l1.quantity = 1.0",
                HashBuilderOperator.class.getSimpleName(),
                Optional.of("INNER;"));
        assertMemoryExceededDetails("" +
                        "SELECT " +
                        "   * " +
                        "FROM (" +
                        " SELECT * " +
                        " FROM lineitem " +
                        " WHERE quantity = 1.0 " +
                        ") l1 " +
                        "RIGHT OUTER JOIN lineitem l2 " +
                        "ON l1.linenumber = l2.linenumber ",
                HashBuilderOperator.class.getSimpleName(),
                Optional.of("RIGHT;"));
    }

    @Test(invocationCount = INVOCATION_COUNT)
    public void testWindow()
    {
        assertMemoryExceededDetails("" +
                        "SELECT " +
                        "   rank() OVER (ORDER BY comment DESC) AS rnk " +
                        "FROM lineitem",
                WindowOperator.class.getSimpleName(),
                Optional.empty());
    }

    private void assertMemoryExceededDetails(String sql, String expectedTopConsumerOperatorName, Optional<String> expectedTopConsumerOperatorInfo)
    {
        try {
            getQueryRunner().execute(getSession(), sql);
            fail("query expected to fail");
        }
        catch (RuntimeException e) {
            Pattern p = Pattern.compile(".*Query exceeded per-node total memory limit of.*, Details: (.*)", DOTALL);
            String message = e.getMessage();
            Matcher matcher = p.matcher(message);
            if (!matcher.matches()) {
                fail("Unexpected error message: " + message);
            }
            String detailsJson = matcher.group(1);
            List<TaskMemoryReservationSummary> summaries = listJsonCodec(TaskMemoryReservationSummary.class).fromJson(detailsJson);
            assertEquals(summaries.get(0).getTopConsumers().get(0).getType(), expectedTopConsumerOperatorName);
            if (expectedTopConsumerOperatorInfo.isPresent()) {
                assertTrue(summaries.get(0).getTopConsumers().get(0).getInfo().isPresent());
                assertThat(summaries.get(0).getTopConsumers().get(0).getInfo().get()).contains(expectedTopConsumerOperatorInfo.get());
            }
        }
    }
}
