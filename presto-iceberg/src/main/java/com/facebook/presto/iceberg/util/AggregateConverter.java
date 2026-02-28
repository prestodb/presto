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

import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;

import java.util.Map;
import java.util.function.Predicate;

public class AggregateConverter
{
    private final Map<Predicate<FunctionHandle>, Expression.Operation> allowedFunctions;

    public AggregateConverter(Map<Predicate<FunctionHandle>, Expression.Operation> allowedFunctions)
    {
        this.allowedFunctions = allowedFunctions;
    }

    public Expression convert(AggregationNode.Aggregation aggregation)
    {
        Expression.Operation operation = this.allowedFunctions.entrySet().stream()
                .filter(entry -> entry.getKey().test(aggregation.getFunctionHandle()))
                .map(entry -> entry.getValue())
                .findFirst().orElse(null);
        if (operation != null) {
            switch (operation) {
                case COUNT:
                    //assert (countAgg.column() instanceof NamedReference);
                    if (!aggregation.getArguments().isEmpty()) {
                        String columnName = ((VariableReferenceExpression) aggregation.getArguments().get(0)).getName();
                        return Expressions.count(columnName);
                    }
                    else {
                        return Expressions.countStar();
                    }
                case MAX:
                    String columnName = ((VariableReferenceExpression) aggregation.getArguments().get(0)).getName();
                    return Expressions.max(columnName);
                case MIN:
                    String columnName2 = ((VariableReferenceExpression) aggregation.getArguments().get(0)).getName();
                    return Expressions.min(columnName2);
            }
        }

        throw new UnsupportedOperationException("Unsupported aggregate: " + aggregation.getFunctionHandle().getName());
    }
}
