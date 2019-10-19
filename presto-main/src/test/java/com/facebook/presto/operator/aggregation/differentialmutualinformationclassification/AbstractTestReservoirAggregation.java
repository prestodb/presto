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
package com.facebook.presto.operator.aggregation.differentialmutualinformationclassification;

import com.facebook.presto.operator.aggregation.AbstractTestAggregationFunction;

abstract class AbstractTestReservoirAggregation
        extends AbstractTestAggregationFunction
{
    protected static final int MAX_SAMPLES = 500;
    protected static final int LENGTH_FACTOR = 8;
    protected static final int TRUE_FACTOR = 4;

    @Override
    protected String getFunctionName()
    {
        return "normalized_differential_mutual_information_classification";
    }
}
