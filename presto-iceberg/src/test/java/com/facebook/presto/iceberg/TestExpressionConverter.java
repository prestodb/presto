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
package com.facebook.presto.iceberg;

import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.TupleDomain;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.UUID;

import static com.facebook.presto.common.predicate.TupleDomain.withColumnDomains;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.UuidType.javaUuidToPrestoUuid;
import static com.facebook.presto.iceberg.ExpressionConverter.toIcebergExpression;
import static com.facebook.presto.iceberg.IcebergColumnHandle.primitiveIcebergColumnHandle;
import static org.apache.iceberg.expressions.Expressions.alwaysTrue;
import static org.apache.iceberg.expressions.Expressions.isNull;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

public class TestExpressionConverter
{
    private static final IcebergColumnHandle UUID_COLUMN =
            primitiveIcebergColumnHandle(1, "uuid_field", com.facebook.presto.common.type.UuidType.UUID, Optional.empty());

    @Test
    public void testUuidValuePredicateIsNotPushedDown()
    {
        // Presto's Parquet writer stores uuid bytes in a different order than Iceberg compares them, so evaluating a
        // value predicate against the bounds in a manifest would prune data files that do contain matching rows.
        Domain domain = Domain.singleValue(
                com.facebook.presto.common.type.UuidType.UUID,
                javaUuidToPrestoUuid(UUID.fromString("9c55ef53-837e-4c0d-833b-40dd4c411aff")));

        assertEquals(
                toIcebergExpression(withColumnDomains(ImmutableMap.of(UUID_COLUMN, domain))).toString(),
                alwaysTrue().toString());
    }

    @Test
    public void testUuidNullPredicateIsPushedDown()
    {
        // Iceberg evaluates a null predicate from null counts alone, which the byte order does not affect.
        Domain domain = Domain.onlyNull(com.facebook.presto.common.type.UuidType.UUID);

        assertEquals(
                toIcebergExpression(withColumnDomains(ImmutableMap.of(UUID_COLUMN, domain))).toString(),
                isNull("uuid_field").toString());
    }

    @Test
    public void testValuePredicateOnOtherTypeIsPushedDown()
    {
        IcebergColumnHandle column = primitiveIcebergColumnHandle(2, "id", INTEGER, Optional.empty());
        TupleDomain<IcebergColumnHandle> tupleDomain =
                withColumnDomains(ImmutableMap.of(column, Domain.singleValue(INTEGER, 7L)));

        assertNotEquals(toIcebergExpression(tupleDomain).toString(), alwaysTrue().toString());
    }
}
