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

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.types.Types.DoubleType;
import org.apache.iceberg.types.Types.ListType;
import org.apache.iceberg.types.Types.LongType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StringType;
import org.apache.iceberg.types.Types.TimestampType;
import org.testng.annotations.Test;

import java.util.function.Consumer;

import static com.facebook.presto.iceberg.PartitionFields.parsePartitionField;
import static com.facebook.presto.iceberg.PartitionFields.toPartitionFields;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static com.google.common.collect.Iterables.getOnlyElement;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

public class TestPartitionFields
{
    @Test
    public void testParse()
    {
        assertParse("order_key", partitionSpec(builder -> builder.identity("order_key")));
        assertParse("comment", partitionSpec(builder -> builder.identity("comment")));
        assertParse("year(ts)", partitionSpec(builder -> builder.year("ts")));
        assertParse("month(ts)", partitionSpec(builder -> builder.month("ts")));
        assertParse("day(ts)", partitionSpec(builder -> builder.day("ts")));
        assertParse("hour(ts)", partitionSpec(builder -> builder.hour("ts")));
        assertParse("bucket(order_key, 42)", partitionSpec(builder -> builder.bucket("order_key", 42)));
        assertParse("truncate(comment, 13)", partitionSpec(builder -> builder.truncate("comment", 13)));
        assertParse("truncate(order_key, 88)", partitionSpec(builder -> builder.truncate("order_key", 88)));

        assertInvalid("bucket()", "Invalid partition field declaration: bucket()");
        assertInvalid("abc", "Cannot find source column: abc");
        assertInvalid("notes", "Cannot partition by non-primitive source field: list<string>");
        assertInvalid("bucket(price, 42)", "Cannot bucket by type: double");
        assertInvalid("bucket(notes, 88)", "Cannot bucket by type: list<string>");
        assertInvalid("truncate(ts, 13)", "Cannot truncate type: timestamp");
        assertInvalid("year(order_key)", "Cannot partition type long by year");
    }

    private static void assertParse(String value, PartitionSpec expected)
    {
        assertEquals(expected.fields().size(), 1);
        assertEquals(parseField(value), expected);
        assertEquals(getOnlyElement(toPartitionFields(expected)), value);
    }

    private static void assertInvalid(String value, String message)
    {
        assertThatThrownBy(() -> parseField(value))
                .isInstanceOfAny(
                        IllegalArgumentException.class,
                        UnsupportedOperationException.class,
                        ValidationException.class)
                .hasMessage(message);
    }

    private static PartitionSpec parseField(String value)
    {
        return partitionSpec(builder -> parsePartitionField(builder, value));
    }

    private static PartitionSpec partitionSpec(Consumer<PartitionSpec.Builder> consumer)
    {
        Schema schema = new Schema(
                NestedField.required(1, "order_key", LongType.get()),
                NestedField.required(2, "ts", TimestampType.withoutZone()),
                NestedField.required(3, "price", DoubleType.get()),
                NestedField.optional(4, "comment", StringType.get()),
                NestedField.optional(5, "notes", ListType.ofRequired(6, StringType.get())));

        PartitionSpec.Builder builder = PartitionSpec.builderFor(schema);
        consumer.accept(builder);
        return builder.build();
    }
}
