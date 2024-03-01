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
package com.facebook.presto.operator;

import com.facebook.presto.operator.aggregation.TestPrecisionRecallAggregation;

import java.util.ArrayList;
import java.util.Iterator;

public class TestClassificationRecallAggregation
        extends TestPrecisionRecallAggregation
{
    public TestClassificationRecallAggregation()
    {
        super("classification_recall");
    }

    @Override
    public ArrayList<Double> getExpectedValue(int start, int length)
    {
        final Iterator<BucketResult> iterator =
                TestPrecisionRecallAggregation.getResultsIterator(start, length);
        final ArrayList<Double> expected = new ArrayList<>();
        while (iterator.hasNext()) {
            final BucketResult result = iterator.next();
            final double recall = result.remainingTrueWeight / result.totalTrueWeight;
            expected.add(recall);
        }
        return expected;
    }
}
