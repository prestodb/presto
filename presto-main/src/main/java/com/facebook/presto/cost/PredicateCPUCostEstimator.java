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
package com.facebook.presto.cost;

import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;

public enum PredicateCPUCostEstimator
{;
    public static int estimateCost(RowExpression predicate)
    {
        if (predicate instanceof SpecialFormExpression) {
            int childCost = 0;
            for (RowExpression child : ((SpecialFormExpression) predicate).getArguments()) {
                childCost += estimateCost(child);
            }
            return childCost;
        }
        else if (predicate instanceof CallExpression) {
            CallExpression callExpr = (CallExpression) predicate;
            for (FunctionCategory category : FunctionCategory.values()) {
                if (callExpr.getDisplayName().startsWith(category.getFnFamily())) {
                    return category.getCost();
                }
            }
        }
        return FunctionCategory.OTHER.getCost();
    }

    private enum FunctionCategory
    {
        JSON("json", 100),
        ARRAY("array", 10),
        REGEX("regexp", 100),
        OTHER("", 1);

        private final String fnFamily;
        private final int cost;

        FunctionCategory(String fnFamily, int cost)
        {
            this.fnFamily = fnFamily;
            this.cost = cost;
        }

        public String getFnFamily()
        {
            return fnFamily;
        }

        public int getCost()
        {
            return cost;
        }
    }
}
