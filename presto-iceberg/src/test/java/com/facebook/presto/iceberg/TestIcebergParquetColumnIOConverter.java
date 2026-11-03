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

import com.facebook.presto.common.type.JsonType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.parquet.Field;
import com.facebook.presto.parquet.GroupField;
import com.facebook.presto.parquet.PrimitiveField;
import com.facebook.presto.parquet.VariantField;
import com.google.common.collect.ImmutableList;
import org.apache.parquet.io.ColumnIO;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.iceberg.ColumnIdentity.TypeCategory.PRIMITIVE;
import static com.facebook.presto.iceberg.ColumnIdentity.TypeCategory.STRUCT;
import static com.facebook.presto.parquet.ParquetTypeUtils.getColumnIO;
import static com.facebook.presto.parquet.ParquetTypeUtils.lookupColumnByName;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * Unit tests for {@link IcebergParquetColumnIOConverter#constructField}.
 */
public class TestIcebergParquetColumnIOConverter
{
    private static ColumnIdentity variantIdentity()
    {
        return new ColumnIdentity(1, "data", PRIMITIVE, ImmutableList.of());
    }

    private static ColumnIO variantColumnIO(String schemaDef)
    {
        MessageType schema = MessageTypeParser.parseMessageType(schemaDef);
        MessageColumnIO messageIO = getColumnIO(schema, schema);
        return lookupColumnByName(messageIO, "data");
    }

    @Test
    public void testVariantStandardOrdering()
    {
        ColumnIO columnIO = variantColumnIO(
                "message table { " +
                        "required group data { " +
                        "  required binary value; " +
                        "  required binary metadata; " +
                        "} }");

        Optional<Field> result = IcebergParquetColumnIOConverter.constructField(
                variantIdentity(), JsonType.JSON, columnIO);

        assertTrue(result.isPresent(), "Expected a VariantField to be returned");
        assertTrue(result.get() instanceof VariantField, "Expected VariantField, got: " + result.get().getClass());
    }

    @Test
    public void testVariantReversedOrdering()
    {
        // metadata listed first, as some writers do
        ColumnIO columnIO = variantColumnIO(
                "message table { " +
                        "required group data { " +
                        "  required binary metadata; " +
                        "  required binary value; " +
                        "} }");

        Optional<Field> result = IcebergParquetColumnIOConverter.constructField(
                variantIdentity(), JsonType.JSON, columnIO);

        assertTrue(result.isPresent(), "Expected a VariantField even with reversed child order");
        assertTrue(result.get() instanceof VariantField,
                "Expected VariantField regardless of physical child ordering, got: " + result.get().getClass());

        VariantField variantField = (VariantField) result.get();

        assertTrue(variantField.getValue() instanceof PrimitiveField, "value sub-field must be a PrimitiveField");
        assertTrue(variantField.getMetadata() instanceof PrimitiveField, "metadata sub-field must be a PrimitiveField");

        String[] valuePath = ((PrimitiveField) variantField.getValue()).getDescriptor().getPath();
        String[] metadataPath = ((PrimitiveField) variantField.getMetadata()).getDescriptor().getPath();

        assertEquals(valuePath[valuePath.length - 1], "value",
                "getValue() must be wired to the physical 'value' column, not 'metadata'");
        assertEquals(metadataPath[metadataPath.length - 1], "metadata",
                "getMetadata() must be wired to the physical 'metadata' column, not 'value'");
    }

    @Test(expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp = ".*[Vv]alue.*")
    public void testVariantMissingValueField()
    {
        ColumnIO columnIO = variantColumnIO(
                "message table { " +
                        "required group data { " +
                        "  required binary metadata; " +
                        "} }");

        IcebergParquetColumnIOConverter.constructField(variantIdentity(), JsonType.JSON, columnIO);
    }

    @Test(expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp = ".*[Mm]etadata.*")
    public void testVariantMissingMetadataField()
    {
        ColumnIO columnIO = variantColumnIO(
                "message table { " +
                        "required group data { " +
                        "  required binary value; " +
                        "} }");

        IcebergParquetColumnIOConverter.constructField(variantIdentity(), JsonType.JSON, columnIO);
    }

    @Test(expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp = ".*[Ss]hredded.*|.*typed_value.*")
    public void testVariantShreddedRejected()
    {
        ColumnIO columnIO = variantColumnIO(
                "message table { " +
                        "required group data { " +
                        "  required binary metadata; " +
                        "  required binary value; " +
                        "  optional binary typed_value; " +
                        "} }");

        IcebergParquetColumnIOConverter.constructField(variantIdentity(), JsonType.JSON, columnIO);
    }

    @Test(expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp = ".*[Ss]hredded.*|.*typed_value.*")
    public void testVariantShreddedRejectedReversedOrder()
    {
        ColumnIO columnIO = variantColumnIO(
                "message table { " +
                        "required group data { " +
                        "  optional binary typed_value; " +
                        "  required binary value; " +
                        "  required binary metadata; " +
                        "} }");

        IcebergParquetColumnIOConverter.constructField(variantIdentity(), JsonType.JSON, columnIO);
    }

    private static ColumnIO infoColumnIO()
    {
        MessageType schema = MessageTypeParser.parseMessageType(
                "message table { " +
                        "required group info { " +
                        "  required int64 old_a = 10; " +
                        "  required int64 old_b = 11; " +
                        "} }");
        MessageColumnIO messageIO = getColumnIO(schema, schema);
        return lookupColumnByName(messageIO, "info");
    }

    @Test(expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp = ".*ColumnIdentity children count.*does not match ROW field count.*")
    public void testRowChildIdentityCountMismatchThrows()
    {
        ColumnIO columnIO = infoColumnIO();
        RowType rowType = RowType.from(ImmutableList.of(RowType.field("a", BIGINT), RowType.field("b", BIGINT)));
        ColumnIdentity mismatchedIdentity = new ColumnIdentity(1, "info", STRUCT,
                ImmutableList.of(new ColumnIdentity(10, "a", PRIMITIVE, ImmutableList.of())));

        IcebergParquetColumnIOConverter.constructField(mismatchedIdentity, rowType, columnIO);
    }

    /** Control for {@link #testRowChildIdentityCountMismatchThrows}: matching counts bind {@code a}/{@code b} to {@code old_a}/{@code old_b} by ID. */
    @Test
    public void testRowResolvedByFieldIdWhenCountsMatch()
    {
        ColumnIO columnIO = infoColumnIO();
        RowType rowType = RowType.from(ImmutableList.of(RowType.field("a", BIGINT), RowType.field("b", BIGINT)));
        ColumnIdentity identity = new ColumnIdentity(1, "info", STRUCT,
                ImmutableList.of(
                        new ColumnIdentity(10, "a", PRIMITIVE, ImmutableList.of()),
                        new ColumnIdentity(11, "b", PRIMITIVE, ImmutableList.of())));

        Optional<Field> result = IcebergParquetColumnIOConverter.constructField(identity, rowType, columnIO);

        assertTrue(result.isPresent(), "Expected ROW to be resolved when identity count matches field count");
        assertTrue(result.get() instanceof GroupField, "Expected GroupField, got: " + result.get().getClass());
        GroupField group = (GroupField) result.get();
        assertEquals(group.getChildren().size(), 2);
        assertEquals(physicalLeafName(group.getChildren().get(0)), "old_a");
        assertEquals(physicalLeafName(group.getChildren().get(1)), "old_b");
    }

    private static String physicalLeafName(Optional<Field> field)
    {
        assertTrue(field.isPresent() && field.get() instanceof PrimitiveField, "Expected a PrimitiveField child");
        String[] path = ((PrimitiveField) field.get()).getDescriptor().getPath();
        return path[path.length - 1];
    }
}
