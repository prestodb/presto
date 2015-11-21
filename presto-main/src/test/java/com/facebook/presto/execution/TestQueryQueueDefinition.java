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
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.testing.TestingSession;
import org.testng.annotations.Test;

import java.util.Optional;

import static org.testng.Assert.assertEquals;

public class TestQueryQueueDefinition
{
    @Test
    public void testNameExpansion()
    {
        Session session = TestingSession.testSessionBuilder()
                .setIdentity(new Identity("bob", Optional.empty()))
                .setSource("the-internet")
                .build();

        QueryQueueDefinition definition = new QueryQueueDefinition("user.${USER}", 1, 1);
        assertEquals(definition.getExpandedTemplate(session), "user.bob");
        definition = new QueryQueueDefinition("source.${SOURCE}", 1, 1);
        assertEquals(definition.getExpandedTemplate(session), "source.the-internet");
        definition = new QueryQueueDefinition("${USER}.${SOURCE}", 1, 1);
        assertEquals(definition.getExpandedTemplate(session), "bob.the-internet");
        definition = new QueryQueueDefinition("global", 1, 1);
        assertEquals(definition.getExpandedTemplate(session), "global");
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = ".*Unsupported template parameter: \\$\\{FOO\\}.*")
    public void testInvalidTemplate()
    {
        new QueryQueueDefinition("user.${FOO}", 1, 1);
    }
}
