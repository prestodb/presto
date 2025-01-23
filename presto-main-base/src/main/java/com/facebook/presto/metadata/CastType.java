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
package com.facebook.presto.metadata;

import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.function.OperatorType;

import static com.facebook.presto.metadata.BuiltInTypeAndFunctionNamespaceManager.DEFAULT_NAMESPACE;
import static com.facebook.presto.operator.scalar.JsonStringToArrayCast.JSON_STRING_TO_ARRAY_NAME;
import static com.facebook.presto.operator.scalar.JsonStringToMapCast.JSON_STRING_TO_MAP_NAME;
import static com.facebook.presto.operator.scalar.JsonStringToRowCast.JSON_STRING_TO_ROW_NAME;
import static com.facebook.presto.operator.scalar.TryCastFunction.TRY_CAST_NAME;
import static java.lang.String.format;

public enum CastType
{
    CAST(OperatorType.CAST.getFunctionName(), true),
    SATURATED_FLOOR_CAST(OperatorType.SATURATED_FLOOR_CAST.getFunctionName(), true),
    TRY_CAST(QualifiedObjectName.valueOf(DEFAULT_NAMESPACE, TRY_CAST_NAME), false),
    JSON_TO_ARRAY_CAST(QualifiedObjectName.valueOf(DEFAULT_NAMESPACE, JSON_STRING_TO_ARRAY_NAME), false),
    JSON_TO_MAP_CAST(QualifiedObjectName.valueOf(DEFAULT_NAMESPACE, JSON_STRING_TO_MAP_NAME), false),
    JSON_TO_ROW_CAST(QualifiedObjectName.valueOf(DEFAULT_NAMESPACE, JSON_STRING_TO_ROW_NAME), false);

    private final QualifiedObjectName castName;
    private final boolean isOperatorType;

    CastType(QualifiedObjectName castName, boolean isOperatorType)
    {
        this.castName = castName;
        this.isOperatorType = isOperatorType;
    }

    public QualifiedObjectName getCastName()
    {
        return castName;
    }

    public boolean isOperatorType()
    {
        return isOperatorType;
    }

    public static OperatorType toOperatorType(CastType castType)
    {
        switch (castType) {
            case CAST:
                return OperatorType.CAST;
            case SATURATED_FLOOR_CAST:
                return OperatorType.SATURATED_FLOOR_CAST;
            default:
                throw new IllegalArgumentException(format("No OperatorType for CastType %s", castType));
        }
    }
}
