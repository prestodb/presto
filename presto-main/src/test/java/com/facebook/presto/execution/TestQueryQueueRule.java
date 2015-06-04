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
import com.facebook.presto.spi.type.TimeZoneKey;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Locale;
import java.util.regex.Pattern;

import static org.testng.Assert.assertEquals;

public class TestQueryQueueRule
{
    @Test
    public void testBasic()
    {
        Session session = new Session("bob", "the-internet", "", "", TimeZoneKey.UTC_KEY, Locale.ENGLISH, null, null, 0, ImmutableMap.of(), ImmutableMap.of());
        QueryQueueDefinition definition = new QueryQueueDefinition("user.${USER}", 1, 1);
        QueryQueueRule rule = new QueryQueueRule(Pattern.compile(".+"), null, ImmutableMap.of(), ImmutableList.of(definition));
        assertEquals(rule.match(session), ImmutableList.of(definition));
    }

    @Test
    public void testBigQuery()
    {
        Session session = new Session("bob", "the-internet", "", "", TimeZoneKey.UTC_KEY, Locale.ENGLISH, null, null, 0, ImmutableMap.of("big_query", "true"), ImmutableMap.of());
        QueryQueueDefinition definition = new QueryQueueDefinition("big", 1, 1);
        QueryQueueRule rule = new QueryQueueRule(null, null, ImmutableMap.of("big_query", Pattern.compile("true", Pattern.CASE_INSENSITIVE)), ImmutableList.of(definition));
        assertEquals(rule.match(session), ImmutableList.of(definition));
    }
}
