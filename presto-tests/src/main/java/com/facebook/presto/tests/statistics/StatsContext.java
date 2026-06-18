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
package com.facebook.presto.tests.statistics;

import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;

public class StatsContext
{
    private final Map<String, VariableReferenceExpression> columnVariables;

    public StatsContext(Map<String, VariableReferenceExpression> columnVariables)
    {
        this.columnVariables = ImmutableMap.copyOf(columnVariables);
    }

    public VariableReferenceExpression getVariableForColumn(String columnName)
    {
        checkArgument(columnVariables.containsKey(columnName), "no variable found for column '" + columnName + "'");
        return columnVariables.get(columnName);
    }
}
