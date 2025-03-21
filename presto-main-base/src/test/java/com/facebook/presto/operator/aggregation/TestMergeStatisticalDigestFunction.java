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
package com.facebook.presto.operator.aggregation;

import com.facebook.presto.common.Page;
import org.testng.annotations.Test;

import java.util.List;
import java.util.function.BiFunction;

import static com.facebook.presto.operator.aggregation.AggregationTestUtils.assertAggregation;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;

public abstract class TestMergeStatisticalDigestFunction
        extends AbstractTestAggregationFunction
{
    protected TestMergeStatisticalDigestFunction()
    {
        super(testSessionBuilder().setSystemProperty("tdigest_enabled", "true").build());
    }

    protected abstract BiFunction<Object, Object, Boolean> getEquality();

    @Override
    protected String getFunctionName()
    {
        return "merge";
    }

    @Override
    protected abstract List<String> getFunctionParameterTypes();

    @Override
    public abstract Object getExpectedValue(int start, int length);

    // The following tests are overridden because by default simple equality checks are done, which often won't work with
    // digests due to the way they are serialized.
    @Test
    @Override
    public void testMultiplePositions()
    {
        assertAggregation(getFunction(),
                getEquality(),
                "test multiple positions",
                new Page(getSequenceBlocks(0, 5)),
                getExpectedValue(0, 5));
    }

    @Test
    @Override
    public void testMixedNullAndNonNullPositions()
    {
        assertAggregation(getFunction(),
                getEquality(),
                "test mixed null and nonnull position",
                new Page(createAlternatingNullsBlock(getFunction().getParameterTypes(), getSequenceBlocks(0, 10))),
                getExpectedValueIncludingNulls(0, 10, 20));
    }
}
