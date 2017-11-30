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
package com.facebook.presto.resourceGroups;

import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.facebook.presto.spi.resourceGroups.SelectionContext;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

import static org.testng.Assert.assertEquals;

public class TestStaticSelector
{
    @Test
    public void testUserRegex()
    {
        ResourceGroupId resourceGroupId = new ResourceGroupId(new ResourceGroupId("global"), "foo");
        StaticSelector selector = new StaticSelector(Optional.of(Pattern.compile("user.*")), Optional.empty(), Optional.empty(), Optional.empty(), new ResourceGroupIdTemplate("global.foo"));
        assertEquals(selector.match(newSelectionContext("userA", null, ImmutableSet.of("tag1"))), Optional.of(resourceGroupId));
        assertEquals(selector.match(newSelectionContext("userB", "source", ImmutableSet.of())), Optional.of(resourceGroupId));
        assertEquals(selector.match(newSelectionContext("A.user", null, ImmutableSet.of("tag1"))), Optional.empty());
    }

    @Test
    public void testSourceRegex()
    {
        ResourceGroupId resourceGroupId = new ResourceGroupId(new ResourceGroupId("global"), "foo");
        StaticSelector selector = new StaticSelector(Optional.empty(), Optional.of(Pattern.compile(".*source.*")), Optional.empty(), Optional.empty(), new ResourceGroupIdTemplate("global.foo"));
        assertEquals(selector.match(newSelectionContext("userA", null, ImmutableSet.of("tag1"))), Optional.empty());
        assertEquals(selector.match(newSelectionContext("userB", "source", ImmutableSet.of())), Optional.of(resourceGroupId));
        assertEquals(selector.match(newSelectionContext("A.user", "a source b", ImmutableSet.of("tag1"))), Optional.of(resourceGroupId));
    }

    @Test
    public void testClientTags()
    {
        ResourceGroupId resourceGroupId = new ResourceGroupId(new ResourceGroupId("global"), "foo");
        StaticSelector selector = new StaticSelector(Optional.empty(), Optional.empty(), Optional.of(ImmutableList.of("tag1", "tag2")), Optional.empty(), new ResourceGroupIdTemplate("global.foo"));
        assertEquals(selector.match(newSelectionContext("userA", null, ImmutableSet.of("tag1", "tag2"))), Optional.of(resourceGroupId));
        assertEquals(selector.match(newSelectionContext("userB", "source", ImmutableSet.of())), Optional.empty());
        assertEquals(selector.match(newSelectionContext("A.user", "a source b", ImmutableSet.of("tag1"))), Optional.empty());
        assertEquals(selector.match(newSelectionContext("A.user", "a source b", ImmutableSet.of("tag1", "tag2", "tag3"))), Optional.of(resourceGroupId));
    }

    private SelectionContext newSelectionContext(String user, String source, Set<String> tags)
    {
        return new SelectionContext(true, user, Optional.ofNullable(source), tags, 1, Optional.empty());
    }
}
