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
package com.facebook.presto.sql.analyzer;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.NodeRef;
import com.facebook.presto.sql.tree.Parameter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;

import java.util.List;
import java.util.Map;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static java.util.Objects.requireNonNull;

public class MultiLineParameters
{
    public static final MultiLineParameters EMPTY = new MultiLineParameters(ImmutableList.of(), ImmutableList.of());

    private final List<NodeRef<Parameter>> parameterRefs;
    private final List<List<Expression>> parameterExpressions;

    public MultiLineParameters(List<NodeRef<Parameter>> parameterRefs, List<List<Expression>> parameterExpressions)
    {
        this.parameterRefs = requireNonNull(parameterRefs, "parameterRefs is null");
        this.parameterExpressions = requireNonNull(parameterExpressions, "parameterExpressions is null");
        for (List<Expression> expressions : parameterExpressions) {
            if (expressions.size() != parameterRefs.size()) {
                throw new PrestoException(INVALID_ARGUMENTS, "Parameters not compatible");
            }
        }
    }

    public static MultiLineParameters from(Map<NodeRef<Parameter>, Expression> parameters)
    {
        requireNonNull(parameters, "parameters is null");
        if (parameters.isEmpty()) {
            return EMPTY;
        }
        ImmutableList.Builder refsBuilder = ImmutableList.builder();
        ImmutableList.Builder expressionsBuilder = ImmutableList.builder();
        parameters.entrySet().stream().forEach(entry -> {
            refsBuilder.add(entry.getKey());
            expressionsBuilder.add(entry.getValue());
        });
        return new MultiLineParameters(refsBuilder.build(), expressionsBuilder.build());
    }

    public int size()
    {
        return parameterExpressions.size();
    }

    public Map<NodeRef<Parameter>, Expression> getFirstRowOfParametersIfExists()
    {
        if (parameterExpressions.isEmpty()) {
            return ImmutableMap.of();
        }
        List<Expression> singleRowExpressions = parameterExpressions.get(0);
        Builder mapBuilder = ImmutableMap.builder();
        for (int i = 0; i < parameterRefs.size(); i++) {
            mapBuilder.put(parameterRefs.get(i), singleRowExpressions.get(i));
        }
        return mapBuilder.build();
    }

    public Map<NodeRef<Parameter>, Expression> getRowOfParameters(int rowIdx)
    {
        if (rowIdx >= parameterExpressions.size()) {
            throw new PrestoException(INVALID_ARGUMENTS, "Invalidate rowIdx: " + rowIdx);
        }

        // rowIdx < 0 implies no parameters exists
        if (rowIdx < 0) {
            return ImmutableMap.of();
        }

        List<Expression> singleRowExpressions = parameterExpressions.get(rowIdx);
        Builder mapBuilder = ImmutableMap.builder();
        for (int i = 0; i < parameterRefs.size(); i++) {
            mapBuilder.put(parameterRefs.get(i), singleRowExpressions.get(i));
        }
        return mapBuilder.build();
    }
}
