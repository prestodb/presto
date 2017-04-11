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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.regex.Pattern;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertEquals;

public class TestQueryQueueRule
{
    @Test
    public void testBasic()
    {
        QueryQueueDefinition definition = new QueryQueueDefinition("user.${USER}", 1, 1);
        QueryQueueRule rule = new QueryQueueRule(Pattern.compile(".+"), null, ImmutableMap.of(), ImmutableList.of(definition));
        assertEquals(rule.match(TEST_SESSION.toSessionRepresentation()).get(), ImmutableList.of(definition));
    }

    @Test
    public void testBigQuery()
    {
        Session session = testSessionBuilder()
                .setSystemProperty("big_query", "true")
                .build();
        QueryQueueDefinition definition = new QueryQueueDefinition("big", 1, 1);
        QueryQueueRule rule = new QueryQueueRule(null, null, ImmutableMap.of("big_query", Pattern.compile("true", Pattern.CASE_INSENSITIVE)), ImmutableList.of(definition));
        assertEquals(rule.match(session.toSessionRepresentation()).get(), ImmutableList.of(definition));
    }
}
