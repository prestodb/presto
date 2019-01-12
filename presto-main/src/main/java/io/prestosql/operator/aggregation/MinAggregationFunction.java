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

public class MinAggregationFunction
        extends AbstractMinMaxAggregationFunction
{
    private static final String NAME = "min";

    public static final MinAggregationFunction MIN_AGGREGATION = new MinAggregationFunction();

    public MinAggregationFunction()
    {
        super(NAME, true);
    }

    @Override
    public String getDescription()
    {
        return "Returns the minimum value of the argument";
    }
}
