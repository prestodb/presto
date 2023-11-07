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
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.common.type.Varchars;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.relation.VariableReferenceExpression;

import java.util.List;

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;

public class CteUtils
{
    private CteUtils()
    {
    }

    // Determines whether the CTE can be materialized.
    public static boolean isCteMaterializable(List<VariableReferenceExpression> outputVariables)
    {
        return outputVariables.stream().anyMatch(CteUtils::isVariableMaterializable)
                && outputVariables.stream()
                .allMatch(variableReferenceExpression -> {
                    if (Varchars.isVarcharType(variableReferenceExpression.getType())) {
                        return isSupportedVarcharType((VarcharType) variableReferenceExpression.getType());
                    }
                    return true;
                });
    }

    /*
        Fetches the index of the first variable that can be materialized.
        ToDo: Implement usage of NDV (number of distinct values) statistics to identify the best partitioning variable,
         as temporary tables are bucketed.
    */
    public static Integer getCtePartitionIndex(List<VariableReferenceExpression> outputVariables)
    {
        for (int i = 0; i < outputVariables.size(); i++) {
            if (isVariableMaterializable(outputVariables.get(i))) {
                return i;
            }
        }
        throw new PrestoException(GENERIC_INTERNAL_ERROR, "No Partitioning index found");
    }

    /*
        Currently, Hive bucketing does not support the Presto type 'ROW'.
    */
    public static boolean isVariableMaterializable(VariableReferenceExpression var)
    {
        return !(var.getType() instanceof RowType);
    }

    /*
        While Presto supports Varchar of length 0 (as discussed in https://github.com/trinodb/trino/issues/1136),
        Hive does not support this.
     */
    private static boolean isSupportedVarcharType(VarcharType varcharType)
    {
        return (varcharType.isUnbounded() || varcharType.getLengthSafe() != 0);
    }
}
