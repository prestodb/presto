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
package com.facebook.presto.execution;

import com.facebook.presto.Session;
import com.facebook.presto.sql.tree.AllColumns;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.Row;
import com.facebook.presto.sql.tree.Statement;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.regex.Pattern;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.sql.QueryUtil.selectList;
import static com.facebook.presto.sql.QueryUtil.simpleQuery;
import static com.facebook.presto.sql.QueryUtil.values;
import static org.testng.Assert.assertEquals;

public class TestQueryQueueRule
{
    private static final Statement STATEMENT = simpleQuery(selectList(new AllColumns()), values(new Row(ImmutableList.of(new LongLiteral("1")))));

    @Test
    public void testBasic()
    {
        QueryQueueDefinition definition = new QueryQueueDefinition("user.${USER}", 1, 1);
        QueryQueueRule rule = new QueryQueueRule(Pattern.compile(".+"), null, ImmutableMap.of(), ImmutableList.of(definition));
        assertEquals(rule.match(STATEMENT, TEST_SESSION.toSessionRepresentation()).get(), ImmutableList.of(definition));
    }

    @Test
    public void testBigQuery()
    {
        Session session = TEST_SESSION.withSystemProperty("big_query", "true");
        QueryQueueDefinition definition = new QueryQueueDefinition("big", 1, 1);
        QueryQueueRule rule = new QueryQueueRule(null, null, ImmutableMap.of("big_query", Pattern.compile("true", Pattern.CASE_INSENSITIVE)), ImmutableList.of(definition));
        assertEquals(rule.match(STATEMENT, session.toSessionRepresentation()).get(), ImmutableList.of(definition));
    }
}
