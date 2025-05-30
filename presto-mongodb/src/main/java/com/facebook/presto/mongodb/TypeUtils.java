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
package com.facebook.presto.mongodb;

import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;

import java.util.function.Predicate;

import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.StandardTypes.JSON;
import static com.facebook.presto.common.type.TimeType.TIME;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;

public final class TypeUtils
{
    private TypeUtils() {}

    public static boolean isArrayType(Type type)
    {
        return type.getTypeSignature().getBase().equals(StandardTypes.ARRAY);
    }
    public static boolean isJsonType(Type type)
    {
        return type.getTypeSignature().getBase().equals(JSON);
    }

    public static boolean isMapType(Type type)
    {
        return type.getTypeSignature().getBase().equals(StandardTypes.MAP);
    }

    public static boolean isRowType(Type type)
    {
        return type.getTypeSignature().getBase().equals(StandardTypes.ROW);
    }

    public static boolean isDateType(Type type)
    {
        return type.equals(DATE) ||
                type.equals(TIME) ||
                type.equals(TIMESTAMP) ||
                type.equals(TIMESTAMP_WITH_TIME_ZONE);
    }

    public static boolean containsType(Type type, Predicate<Type> predicate, Predicate<Type>... orPredicates)
    {
        for (Predicate<Type> orPredicate : orPredicates) {
            predicate = predicate.or(orPredicate);
        }
        if (predicate.test(type)) {
            return true;
        }

        return type.getTypeParameters().stream().anyMatch(predicate);
    }
}
