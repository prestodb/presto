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
package io.prestosql.plugin.tpch.util;

import com.google.common.collect.ImmutableMap;
import io.prestosql.plugin.tpch.TpchColumnHandle;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.predicate.NullableValue;
import io.prestosql.spi.predicate.TupleDomain;

import java.util.function.Predicate;

public final class PredicateUtils
{
    private PredicateUtils() {}

    public static Predicate<NullableValue> convertToPredicate(TupleDomain<ColumnHandle> predicate, TpchColumnHandle columnHandle)
    {
        TupleDomain<ColumnHandle> columnPredicate = filterColumns(predicate, columnHandle::equals);
        return nullableValue -> columnPredicate.contains(TupleDomain.fromFixedValues(ImmutableMap.of(columnHandle, nullableValue)));
    }

    public static TupleDomain<ColumnHandle> filterOutColumnFromPredicate(TupleDomain<ColumnHandle> predicate, TpchColumnHandle columnHandle)
    {
        return filterColumns(predicate, tpchColumnHandle -> !tpchColumnHandle.equals(columnHandle));
    }

    public static TupleDomain<ColumnHandle> filterColumns(TupleDomain<ColumnHandle> predicate, Predicate<TpchColumnHandle> filterPredicate)
    {
        return predicate.transform(columnHandle -> {
            TpchColumnHandle tpchColumnHandle = (TpchColumnHandle) columnHandle;
            if (filterPredicate.test(tpchColumnHandle)) {
                return tpchColumnHandle;
            }

            return null;
        });
    }
}
