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
package io.prestosql.plugin.resourcegroups;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.prestosql.plugin.resourcegroups.SelectorResourceEstimate.Range;
import io.prestosql.spi.resourcegroups.ResourceGroupId;
import io.prestosql.spi.resourcegroups.SelectionContext;
import io.prestosql.spi.resourcegroups.SelectionCriteria;
import io.prestosql.spi.session.ResourceEstimates;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

import static org.testng.Assert.assertEquals;

public class TestStaticSelector
{
    private static final ResourceEstimates EMPTY_RESOURCE_ESTIMATES = new ResourceEstimates(Optional.empty(), Optional.empty(), Optional.empty());

    @Test
    public void testUserRegex()
    {
        ResourceGroupId resourceGroupId = new ResourceGroupId(new ResourceGroupId("global"), "foo");
        StaticSelector selector = new StaticSelector(
                Optional.of(Pattern.compile("user.*")),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                new ResourceGroupIdTemplate("global.foo"));
        assertEquals(selector.match(newSelectionCriteria("userA", null, ImmutableSet.of("tag1"), EMPTY_RESOURCE_ESTIMATES)).map(SelectionContext::getResourceGroupId), Optional.of(resourceGroupId));
        assertEquals(selector.match(newSelectionCriteria("userB", "source", ImmutableSet.of(), EMPTY_RESOURCE_ESTIMATES)).map(SelectionContext::getResourceGroupId), Optional.of(resourceGroupId));
        assertEquals(selector.match(newSelectionCriteria("A.user", null, ImmutableSet.of("tag1"), EMPTY_RESOURCE_ESTIMATES)), Optional.empty());
    }

    @Test
    public void testSourceRegex()
    {
        ResourceGroupId resourceGroupId = new ResourceGroupId(new ResourceGroupId("global"), "foo");
        StaticSelector selector = new StaticSelector(
                Optional.empty(),
                Optional.of(Pattern.compile(".*source.*")),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                new ResourceGroupIdTemplate("global.foo"));
        assertEquals(selector.match(newSelectionCriteria("userA", null, ImmutableSet.of("tag1"), EMPTY_RESOURCE_ESTIMATES)), Optional.empty());
        assertEquals(selector.match(newSelectionCriteria("userB", "source", ImmutableSet.of(), EMPTY_RESOURCE_ESTIMATES)).map(SelectionContext::getResourceGroupId), Optional.of(resourceGroupId));
        assertEquals(selector.match(newSelectionCriteria("A.user", "a source b", ImmutableSet.of("tag1"), EMPTY_RESOURCE_ESTIMATES)).map(SelectionContext::getResourceGroupId), Optional.of(resourceGroupId));
    }

    @Test
    public void testClientTags()
    {
        ResourceGroupId resourceGroupId = new ResourceGroupId(new ResourceGroupId("global"), "foo");
        StaticSelector selector = new StaticSelector(
                Optional.empty(),
                Optional.empty(),
                Optional.of(ImmutableList.of("tag1", "tag2")),
                Optional.empty(),
                Optional.empty(),
                new ResourceGroupIdTemplate("global.foo"));
        assertEquals(selector.match(newSelectionCriteria("userA", null, ImmutableSet.of("tag1", "tag2"), EMPTY_RESOURCE_ESTIMATES)).map(SelectionContext::getResourceGroupId), Optional.of(resourceGroupId));
        assertEquals(selector.match(newSelectionCriteria("userB", "source", ImmutableSet.of(), EMPTY_RESOURCE_ESTIMATES)), Optional.empty());
        assertEquals(selector.match(newSelectionCriteria("A.user", "a source b", ImmutableSet.of("tag1"), EMPTY_RESOURCE_ESTIMATES)), Optional.empty());
        assertEquals(selector.match(newSelectionCriteria("A.user", "a source b", ImmutableSet.of("tag1", "tag2", "tag3"), EMPTY_RESOURCE_ESTIMATES)).map(SelectionContext::getResourceGroupId), Optional.of(resourceGroupId));
    }

    @Test
    public void testSelectorResourceEstimate()
    {
        ResourceGroupId resourceGroupId = new ResourceGroupId(new ResourceGroupId("global"), "foo");

        StaticSelector smallQuerySelector = new StaticSelector(
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(new SelectorResourceEstimate(
                        Optional.of(new Range<>(
                                Optional.empty(),
                                Optional.of(Duration.valueOf("5m")))),
                        Optional.empty(),
                        Optional.of(new Range<>(
                                Optional.empty(),
                                Optional.of(DataSize.valueOf("500MB")))))),
                Optional.empty(),
                new ResourceGroupIdTemplate("global.foo"));

        assertEquals(
                smallQuerySelector.match(
                        newSelectionCriteria(
                                "userA",
                                null,
                                ImmutableSet.of("tag1", "tag2"),
                                new ResourceEstimates(
                                        Optional.of(Duration.valueOf("4m")),
                                        Optional.empty(),
                                        Optional.of(DataSize.valueOf("400MB")))))
                        .map(SelectionContext::getResourceGroupId),
                Optional.of(resourceGroupId));

        assertEquals(
                smallQuerySelector.match(
                        newSelectionCriteria(
                                "A.user",
                                "a source b",
                                ImmutableSet.of("tag1"),
                                new ResourceEstimates(
                                        Optional.of(Duration.valueOf("4m")),
                                        Optional.empty(),
                                        Optional.of(DataSize.valueOf("600MB")))))
                        .map(SelectionContext::getResourceGroupId),
                Optional.empty());

        assertEquals(
                smallQuerySelector.match(
                        newSelectionCriteria(
                                "userB",
                                "source",
                                ImmutableSet.of(),
                                new ResourceEstimates(
                                        Optional.of(Duration.valueOf("4m")),
                                        Optional.empty(),
                                        Optional.empty())))
                        .map(SelectionContext::getResourceGroupId),
                Optional.empty());

        StaticSelector largeQuerySelector = new StaticSelector(
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(new SelectorResourceEstimate(
                        Optional.empty(),
                        Optional.empty(),
                        Optional.of(new Range<>(
                                Optional.of(DataSize.valueOf("5TB")),
                                Optional.empty())))),
                Optional.empty(),
                new ResourceGroupIdTemplate("global.foo"));

        assertEquals(
                largeQuerySelector.match(
                        newSelectionCriteria(
                                "userA",
                                null,
                                ImmutableSet.of("tag1", "tag2"),
                                new ResourceEstimates(
                                        Optional.of(Duration.valueOf("100h")),
                                        Optional.empty(),
                                        Optional.of(DataSize.valueOf("4TB")))))
                        .map(SelectionContext::getResourceGroupId),
                Optional.empty());

        assertEquals(
                largeQuerySelector.match(
                        newSelectionCriteria(
                                "A.user",
                                "a source b",
                                ImmutableSet.of("tag1"),
                                new ResourceEstimates(
                                        Optional.empty(),
                                        Optional.empty(),
                                        Optional.of(DataSize.valueOf("6TB")))))
                        .map(SelectionContext::getResourceGroupId),
                Optional.of(resourceGroupId));

        assertEquals(
                largeQuerySelector.match(
                        newSelectionCriteria(
                                "userB",
                                "source",
                                ImmutableSet.of(),
                                new ResourceEstimates(
                                        Optional.of(Duration.valueOf("1s")),
                                        Optional.of(Duration.valueOf("1s")),
                                        Optional.of(DataSize.valueOf("6TB")))))
                        .map(SelectionContext::getResourceGroupId),
                Optional.of(resourceGroupId));
    }

    private SelectionCriteria newSelectionCriteria(String user, String source, Set<String> tags, ResourceEstimates resourceEstimates)
    {
        return new SelectionCriteria(true, user, Optional.ofNullable(source), tags, resourceEstimates, Optional.empty());
    }
}
