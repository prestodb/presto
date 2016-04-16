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
package com.facebook.presto.execution.resourceGroups;

import com.facebook.presto.Session;
import com.facebook.presto.spi.security.Identity;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertEquals;

public class TestResourceGroupIdTemplate
{
    @Test
    public void testExpansion()
    {
        ResourceGroupIdTemplate template = new ResourceGroupIdTemplate("test.${USER}.${SOURCE}");
        Session session = testSessionBuilder()
                .setSource("s")
                .setIdentity(new Identity("u", Optional.empty()))
                .build();
        assertEquals(template.expandTemplate(session.toSessionRepresentation()), ResourceGroupId.fromString("test.u.s"));
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testInvalid()
    {
        new ResourceGroupIdTemplate("test.${USER}.${SOURCE}.${BROKEN}");
    }
}
