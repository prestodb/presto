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

import com.facebook.presto.resourceGroups.SelectorResourceEstimate.Range;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.facebook.presto.spi.resourceGroups.SelectionContext;
import com.facebook.presto.spi.resourceGroups.SelectionCriteria;
import com.facebook.presto.spi.session.ResourceEstimates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.time.LocalDateTime;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

import static org.testng.Assert.assertEquals;

public class TestStaticSelector
{
    private static final ResourceEstimates EMPTY_RESOURCE_ESTIMATES = new ResourceEstimates(Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());

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
                Optional.empty(),
                new ResourceGroupIdTemplate("global.foo"));
        assertEquals(selector.match(newSelectionCriteria("userA", null, ImmutableSet.of("tag1"), null, EMPTY_RESOURCE_ESTIMATES)).map(SelectionContext::getResourceGroupId), Optional.of(resourceGroupId));
        assertEquals(selector.match(newSelectionCriteria("userB", "source", ImmutableSet.of(), null, EMPTY_RESOURCE_ESTIMATES)).map(SelectionContext::getResourceGroupId), Optional.of(resourceGroupId));
        assertEquals(selector.match(newSelectionCriteria("A.user", null, ImmutableSet.of("tag1"), null, EMPTY_RESOURCE_ESTIMATES)), Optional.empty());
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
                Optional.empty(),
                new ResourceGroupIdTemplate("global.foo"));

        assertEquals(selector.match(newSelectionCriteria("userA", null, ImmutableSet.of("tag1"), null, EMPTY_RESOURCE_ESTIMATES)), Optional.empty());
        assertEquals(selector.match(newSelectionCriteria("userB", "source", ImmutableSet.of(), null, EMPTY_RESOURCE_ESTIMATES)).map(SelectionContext::getResourceGroupId), Optional.of(resourceGroupId));
        assertEquals(selector.match(newSelectionCriteria("A.user", "a source b", ImmutableSet.of("tag1"), null, EMPTY_RESOURCE_ESTIMATES)).map(SelectionContext::getResourceGroupId), Optional.of(resourceGroupId));
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
                Optional.empty(),
                new ResourceGroupIdTemplate("global.foo"));
        assertEquals(selector.match(newSelectionCriteria("userA", null, ImmutableSet.of("tag1", "tag2"), null, EMPTY_RESOURCE_ESTIMATES)).map(SelectionContext::getResourceGroupId), Optional.of(resourceGroupId));
        assertEquals(selector.match(newSelectionCriteria("userB", "source", ImmutableSet.of(), null, EMPTY_RESOURCE_ESTIMATES)), Optional.empty());
        assertEquals(selector.match(newSelectionCriteria("A.user", "a source b", ImmutableSet.of("tag1"), null, EMPTY_RESOURCE_ESTIMATES)), Optional.empty());
        assertEquals(selector.match(newSelectionCriteria("A.user", "a source b", ImmutableSet.of("tag1", "tag2", "tag3"), null, EMPTY_RESOURCE_ESTIMATES)).map(SelectionContext::getResourceGroupId), Optional.of(resourceGroupId));
    }

    @Test
    public void testSelectorTimeOfDay()
    {
        ResourceGroupId resourceGroupId1 = new ResourceGroupId(new ResourceGroupId("global"), "foo1");
        ResourceGroupId resourceGroupId2 = new ResourceGroupId(new ResourceGroupId("global"), "foo2");

        String startTime = "12:00";
        String endTime1 = "17:00";
        String endTime2 = "02:00";

        StaticSelector selector1 = new StaticSelector(
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(new SelectorTimeOfDay(startTime, endTime1)),
                Optional.empty(),
                Optional.empty(),
                new ResourceGroupIdTemplate("global.foo1"));
        StaticSelector selector2 = new StaticSelector(
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(new SelectorTimeOfDay(startTime, endTime2)),
                Optional.empty(),
                Optional.empty(),
                new ResourceGroupIdTemplate("global.foo2"));

        assertEquals(selector1.match(newSelectionCriteria("userA", null, ImmutableSet.of(), LocalDateTime.parse("1986-04-08T12:30"), EMPTY_RESOURCE_ESTIMATES)).map(SelectionContext::getResourceGroupId), Optional.of(resourceGroupId1));
        assertEquals(selector1.match(newSelectionCriteria("userB", null, ImmutableSet.of(), LocalDateTime.parse("1986-04-08T11:30"), EMPTY_RESOURCE_ESTIMATES)).map(SelectionContext::getResourceGroupId), Optional.empty());
        assertEquals(selector2.match(newSelectionCriteria("userA", null, ImmutableSet.of(), LocalDateTime.parse("1986-04-08T03:30"), EMPTY_RESOURCE_ESTIMATES)).map(SelectionContext::getResourceGroupId), Optional.empty());
        assertEquals(selector2.match(newSelectionCriteria("userB", null, ImmutableSet.of(), LocalDateTime.parse("1986-04-08T11:30"), EMPTY_RESOURCE_ESTIMATES)).map(SelectionContext::getResourceGroupId), Optional.empty());
        assertEquals(selector2.match(newSelectionCriteria("userC", null, ImmutableSet.of(), LocalDateTime.parse("1986-04-08T01:30"), EMPTY_RESOURCE_ESTIMATES)).map(SelectionContext::getResourceGroupId), Optional.of(resourceGroupId2));
        assertEquals(selector2.match(newSelectionCriteria("userD", null, ImmutableSet.of(), LocalDateTime.parse("1986-04-08T17:30"), EMPTY_RESOURCE_ESTIMATES)).map(SelectionContext::getResourceGroupId), Optional.of(resourceGroupId2));
    }

    @Test
    public void testSelectorResourceEstimate()
    {
        ResourceGroupId resourceGroupId = new ResourceGroupId(new ResourceGroupId("global"), "foo");

        StaticSelector smallQuerySelector = new StaticSelector(
                Optional.empty(),
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
                                null,
                                new ResourceEstimates(
                                        Optional.of(Duration.valueOf("4m")),
                                        Optional.empty(),
                                        Optional.of(DataSize.valueOf("400MB")),
                                        Optional.empty())))
                        .map(SelectionContext::getResourceGroupId),
                Optional.of(resourceGroupId));

        assertEquals(
                smallQuerySelector.match(
                        newSelectionCriteria(
                                "A.user",
                                "a source b",
                                ImmutableSet.of("tag1"),
                                null,
                                new ResourceEstimates(
                                        Optional.of(Duration.valueOf("4m")),
                                        Optional.empty(),
                                        Optional.of(DataSize.valueOf("600MB")),
                                        Optional.empty())))
                        .map(SelectionContext::getResourceGroupId),
                Optional.empty());

        assertEquals(
                smallQuerySelector.match(
                        newSelectionCriteria(
                                "userB",
                                "source",
                                ImmutableSet.of(),
                                null,
                                new ResourceEstimates(
                                        Optional.of(Duration.valueOf("4m")),
                                        Optional.empty(),
                                        Optional.empty(),
                                        Optional.empty())))
                        .map(SelectionContext::getResourceGroupId),
                Optional.empty());

        StaticSelector largeQuerySelector = new StaticSelector(
                Optional.empty(),
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
                                null,
                                new ResourceEstimates(
                                        Optional.of(Duration.valueOf("100h")),
                                        Optional.empty(),
                                        Optional.of(DataSize.valueOf("4TB")),
                                        Optional.empty())))
                        .map(SelectionContext::getResourceGroupId),
                Optional.empty());

        assertEquals(
                largeQuerySelector.match(
                        newSelectionCriteria(
                                "A.user",
                                "a source b",
                                ImmutableSet.of("tag1"),
                                null,
                                new ResourceEstimates(
                                        Optional.empty(),
                                        Optional.empty(),
                                        Optional.of(DataSize.valueOf("6TB")),
                                        Optional.empty())))
                        .map(SelectionContext::getResourceGroupId),
                Optional.of(resourceGroupId));

        assertEquals(
                largeQuerySelector.match(
                        newSelectionCriteria(
                                "userB",
                                "source",
                                ImmutableSet.of(),
                                null,
                                new ResourceEstimates(
                                        Optional.of(Duration.valueOf("1s")),
                                        Optional.of(Duration.valueOf("1s")),
                                        Optional.of(DataSize.valueOf("6TB")),
                                        Optional.empty())))
                        .map(SelectionContext::getResourceGroupId),
                Optional.of(resourceGroupId));
    }

    private SelectionCriteria newSelectionCriteria(String user, String source, Set<String> tags, LocalDateTime timeOfDay, ResourceEstimates resourceEstimates)
    {
        return new SelectionCriteria(true, user, Optional.ofNullable(source), tags, Optional.ofNullable(timeOfDay), resourceEstimates, Optional.empty());
    }
}
