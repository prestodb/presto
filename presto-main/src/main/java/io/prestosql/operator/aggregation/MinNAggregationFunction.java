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
package io.prestosql.operator.aggregation;

public class MinNAggregationFunction
        extends AbstractMinMaxNAggregationFunction
{
    private static final String NAME = "min";

    public static final MinNAggregationFunction MIN_N_AGGREGATION = new MinNAggregationFunction();

    public MinNAggregationFunction()
    {
        super(NAME, t -> ((leftBlock, leftIndex, rightBlock, rightIndex) -> -t.compareTo(leftBlock, leftIndex, rightBlock, rightIndex)));
    }

    @Override
    public String getDescription()
    {
        return "Returns the minimum values of the argument";
    }
}
