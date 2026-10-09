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
import com.google.common.collect.ImmutableList;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.iceberg.ExpressionConverter.isNullSensitiveOnRequiredField;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestExpressionConverter
{
    private static final List<Domain> NULL_SENSITIVE_DOMAINS = ImmutableList.of(Domain.onlyNull(BIGINT), Domain.notNull(BIGINT));

    @Test
    public void testColumnRequiredInEverySchema()
    {
        List<Schema> schemas = ImmutableList.of(
                new Schema(0, ImmutableList.of(required(1, "c", Types.LongType.get()))),
                new Schema(1, ImmutableList.of(required(1, "c", Types.LongType.get()), optional(2, "d", Types.LongType.get()))));
        assertNotNullSensitive(schemas, "c");
    }

    @Test
    public void testColumnRequiredInSomeSchemas()
    {
        Schema optionalColumn = new Schema(0, ImmutableList.of(optional(1, "c", Types.LongType.get())));
        Schema requiredColumn = new Schema(1, ImmutableList.of(required(1, "c", Types.LongType.get())));
        Schema absentColumn = new Schema(2, ImmutableList.of(optional(2, "d", Types.LongType.get())));

        assertNullSensitive(ImmutableList.of(optionalColumn, requiredColumn), "c");
        assertNullSensitive(ImmutableList.of(requiredColumn, optionalColumn), "c");
        assertNullSensitive(ImmutableList.of(absentColumn, requiredColumn), "c");
        assertNotNullSensitive(ImmutableList.of(optionalColumn), "c");
    }

    @Test
    public void testNestedFieldUnderStruct()
    {
        Schema optionalStruct = new Schema(0, ImmutableList.of(optional(1, "s", Types.StructType.of(required(2, "x", Types.LongType.get())))));
        Schema requiredStruct = new Schema(1, ImmutableList.of(required(1, "s", Types.StructType.of(required(2, "x", Types.LongType.get())))));

        assertNullSensitive(ImmutableList.of(optionalStruct), "s.x");
        assertNullSensitive(ImmutableList.of(optionalStruct, requiredStruct), "s.x");
        assertNotNullSensitive(ImmutableList.of(requiredStruct), "s.x");
    }

    @Test
    public void testRenamedAndReplacedColumns()
    {
        // c is made required and then renamed to d
        assertNullSensitive(
                ImmutableList.of(
                        new Schema(0, ImmutableList.of(optional(1, "c", Types.LongType.get()))),
                        new Schema(1, ImmutableList.of(required(1, "d", Types.LongType.get())))),
                "d");
        // c is dropped and a new required column named c is added
        assertNullSensitive(
                ImmutableList.of(
                        new Schema(0, ImmutableList.of(required(1, "c", Types.LongType.get()))),
                        new Schema(1, ImmutableList.of(required(2, "c", Types.LongType.get())))),
                "c");
    }

    @Test
    public void testDomainWithoutNullPredicate()
    {
        List<Schema> schemas = ImmutableList.of(
                new Schema(0, ImmutableList.of(optional(1, "c", Types.LongType.get()))),
                new Schema(1, ImmutableList.of(required(1, "c", Types.LongType.get()))));
        assertFalse(isNullSensitiveOnRequiredField(schemas, "c", Domain.singleValue(BIGINT, 1L)));
    }

    private static void assertNullSensitive(List<Schema> schemas, String columnName)
    {
        for (Domain domain : NULL_SENSITIVE_DOMAINS) {
            assertTrue(isNullSensitiveOnRequiredField(schemas, columnName, domain), domain.toString());
        }
    }

    private static void assertNotNullSensitive(List<Schema> schemas, String columnName)
    {
        for (Domain domain : NULL_SENSITIVE_DOMAINS) {
            assertFalse(isNullSensitiveOnRequiredField(schemas, columnName, domain), domain.toString());
        }
    }
}
