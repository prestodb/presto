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
package io.prestosql.operator.aggregation.minmaxby;

public class MaxByNAggregationFunction
        extends AbstractMinMaxByNAggregationFunction
{
    private static final String NAME = "max_by";

    public static final MaxByNAggregationFunction MAX_BY_N_AGGREGATION = new MaxByNAggregationFunction();

    public MaxByNAggregationFunction()
    {
        super(NAME, t -> t::compareTo);
    }

    @Override
    public String getDescription()
    {
        return "Returns the values of the first argument associated with the maximum values of the second argument";
    }
}
