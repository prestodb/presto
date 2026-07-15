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
package com.facebook.presto.parquet;

import com.facebook.presto.common.type.RowType;
import com.google.common.collect.ImmutableList;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.parquet.ParquetTypeUtils.getColumnIO;
import static com.facebook.presto.parquet.ParquetTypeUtils.getDescriptors;
import static com.facebook.presto.parquet.ParquetTypeUtils.lookupDescriptor;
import static org.apache.parquet.io.ColumnIOConverter.constructField;
import static org.apache.parquet.schema.LogicalTypeAnnotation.stringType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;

public class TestParquetTypeUtils
{
    private static final RowType CASE_SENSITIVE_ROW_TYPE = RowType.from(ImmutableList.of(
            RowType.field("Status", VARCHAR),
            RowType.field("status", BIGINT)));

    private static final MessageType CASE_SENSITIVE_SCHEMA = Types.buildMessage()
            .optionalGroup()
                .optional(BINARY).as(stringType()).id(115).named("Status")
                .optional(INT64).id(122).named("status")
                .named("response_body")
            .named("schema");

    @Test
    public void testDescriptorsPreferExactCase()
    {
        Map<List<String>, RichColumnDescriptor> descriptors = getDescriptors(CASE_SENSITIVE_SCHEMA, CASE_SENSITIVE_SCHEMA);

        assertEquals(descriptors.get(ImmutableList.of("response_body", "Status")).getPrimitiveType().getPrimitiveTypeName(), BINARY);
        assertEquals(descriptors.get(ImmutableList.of("response_body", "status")).getPrimitiveType().getPrimitiveTypeName(), INT64);
    }

    @Test
    public void testDescriptorsExcludeUnrequestedCaseVariant()
    {
        MessageType requestedSchema = Types.buildMessage()
                .optionalGroup()
                    .optional(BINARY).as(stringType()).id(115).named("Status")
                    .named("response_body")
                .named("schema");

        Map<List<String>, RichColumnDescriptor> descriptors = getDescriptors(CASE_SENSITIVE_SCHEMA, requestedSchema);

        assertEquals(descriptors.size(), 1);
        assertEquals(descriptors.get(ImmutableList.of("response_body", "Status")).getPrimitiveType().getPrimitiveTypeName(), BINARY);
        assertFalse(descriptors.containsKey(ImmutableList.of("response_body", "status")));
    }

    @Test
    public void testDescriptorLookupFallsBackToCaseInsensitiveMatch()
    {
        MessageType mixedCaseSchema = Types.buildMessage()
                .optional(INT64).named("MixedCase")
                .named("schema");
        Map<List<String>, RichColumnDescriptor> descriptors = getDescriptors(mixedCaseSchema, mixedCaseSchema);

        assertEquals(lookupDescriptor(descriptors, ImmutableList.of("mixedcase")).getPrimitiveType().getPrimitiveTypeName(), INT64);
    }

    @Test
    public void testDescriptorLookupRejectsAmbiguousCaseInsensitiveMatch()
    {
        Map<List<String>, RichColumnDescriptor> descriptors = getDescriptors(CASE_SENSITIVE_SCHEMA, CASE_SENSITIVE_SCHEMA);

        assertNull(lookupDescriptor(descriptors, ImmutableList.of("RESPONSE_BODY", "STATUS")));
    }

    @Test
    public void testConstructFieldPreservesCase()
    {
        MessageColumnIO columnIO = getColumnIO(CASE_SENSITIVE_SCHEMA, CASE_SENSITIVE_SCHEMA);
        GroupField field = (GroupField) constructField(CASE_SENSITIVE_ROW_TYPE, columnIO.getChild("response_body")).get();

        PrimitiveField upperCaseField = (PrimitiveField) field.getChildren().get(0).get();
        PrimitiveField lowerCaseField = (PrimitiveField) field.getChildren().get(1).get();
        assertEquals(upperCaseField.getDescriptor().getPrimitiveType().getPrimitiveTypeName(), BINARY);
        assertEquals(lowerCaseField.getDescriptor().getPrimitiveType().getPrimitiveTypeName(), INT64);
    }
}
