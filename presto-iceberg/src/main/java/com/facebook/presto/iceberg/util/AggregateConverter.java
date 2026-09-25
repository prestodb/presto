/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.facebook.presto.iceberg.util;

import com.facebook.presto.iceberg.IcebergColumnHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;

import java.util.Map;
import java.util.function.Predicate;

import static com.google.common.collect.Iterables.getOnlyElement;

public class AggregateConverter
{
    private final Map<Predicate<FunctionHandle>, Expression.Operation> allowedFunctions;

    public AggregateConverter(Map<Predicate<FunctionHandle>, Expression.Operation> allowedFunctions)
    {
        this.allowedFunctions = allowedFunctions;
    }

    public Expression convert(AggregationNode.Aggregation aggregation,
                              Map<VariableReferenceExpression, ColumnHandle> tableScanAssignments)
    {
        Expression.Operation operation = this.allowedFunctions.entrySet().stream()
                .filter(entry -> entry.getKey().test(aggregation.getFunctionHandle()))
                .map(Map.Entry::getValue)
                .findFirst()
                .orElse(null);

        if (operation != null) {
            switch (operation) {
                case COUNT:
                    if (!aggregation.getArguments().isEmpty()) {
                        return Expressions.count(getColumnNameOfAggregation(aggregation, tableScanAssignments));
                    }
                    else {
                        return Expressions.countStar();
                    }
                case MAX:
                    return Expressions.max(getColumnNameOfAggregation(aggregation, tableScanAssignments));
                case MIN:
                    return Expressions.min(getColumnNameOfAggregation(aggregation, tableScanAssignments));
            }
        }

        throw new UnsupportedOperationException("Unsupported aggregate: " + aggregation.getFunctionHandle().getName());
    }

    private static String getColumnNameOfAggregation(AggregationNode.Aggregation aggregation,
                                                     Map<VariableReferenceExpression, ColumnHandle> tableScanAssignments)
    {
        VariableReferenceExpression variable = (VariableReferenceExpression) getOnlyElement(aggregation.getArguments());
        ColumnHandle columnHandle = tableScanAssignments.get(variable);
        if (columnHandle == null) {
            throw new IllegalArgumentException("Cannot find corresponding column for variable: " + variable.toString());
        }
        return ((IcebergColumnHandle) columnHandle).getName();
    }
}
