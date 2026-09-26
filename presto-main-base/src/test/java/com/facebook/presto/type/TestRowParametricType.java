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
package com.facebook.presto.type;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.json.JsonCodecFactory;
import com.facebook.airlift.json.JsonObjectMapperProvider;
import com.facebook.airlift.json.ObjectMapperProvider;
import com.facebook.presto.common.type.NamedTypeSignature;
import com.facebook.presto.common.type.RowFieldName;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeParameter;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.common.type.TypeSignatureParameter;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.common.type.StandardTypes.BIGINT;
import static com.facebook.presto.common.type.StandardTypes.DOUBLE;
import static com.facebook.presto.common.type.StandardTypes.ROW;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestRowParametricType
{
    /**
     * Shared {@link JsonCodec} for {@link RowType.Field} wired with the standard
     * {@link TypeDeserializer}. Initialized once per test class to avoid re-creating
     * the mapper on every test invocation.
     */
    private static final JsonCodec<RowType.Field> FIELD_CODEC = buildFieldCodec();

    private static JsonCodec<RowType.Field> buildFieldCodec()
    {
        ObjectMapperProvider mapperProvider = new JsonObjectMapperProvider();
        mapperProvider.setJsonDeserializers(ImmutableMap.of(Type.class, new TypeDeserializer(createTestFunctionAndTypeManager())));
        return new JsonCodecFactory(mapperProvider).jsonCodec(RowType.Field.class);
    }

    @Test
    public void testTypeSignatureRoundTrip()
    {
        FunctionAndTypeManager functionAndTypeManager = createTestFunctionAndTypeManager();
        TypeSignature typeSignature = new TypeSignature(
                ROW,
                TypeSignatureParameter.of(new NamedTypeSignature(Optional.of(new RowFieldName("col1", false)), new TypeSignature(BIGINT))),
                TypeSignatureParameter.of(new NamedTypeSignature(Optional.of(new RowFieldName("col2", true)), new TypeSignature(DOUBLE))));
        List<TypeParameter> parameters = typeSignature.getParameters().stream()
                .map(parameter -> TypeParameter.of(parameter, functionAndTypeManager))
                .collect(Collectors.toList());
        Type rowType = RowParametricType.ROW.createType(parameters);

        assertEquals(rowType.getTypeSignature(), typeSignature);
    }

    /**
     * Regression test for issue 28141.
     *
     * RowType.Field with delimited=true must survive a Jackson serialize→deserialize
     * round-trip. The bug: @JsonCreator on the 2-arg constructor only knew "name" and
     * "type", so the "delimited" property was silently dropped on deserialization.
     * After that, getTypeSignature().toString() emitted "row(... varchar)" (unquoted),
     * and any parseTypeSignature() call on it threw:
     *   IllegalArgumentException: Bad type signature: 'row(... varchar)'.
     */
    @Test
    public void testDelimitedFieldNamePreservedAfterJsonRoundTrip()
    {
        RowType.Field original = new RowType.Field(Optional.of("..."), VARCHAR, true);

        RowType.Field deserialized = FIELD_CODEC.fromJson(FIELD_CODEC.toJson(original));
        assertTrue(deserialized.isDelimited(), "delimited flag must survive Jackson round-trip");
        RowType rowType = RowType.from(Arrays.asList(deserialized));
        // Verify the signature is parseable — the bug caused parseTypeSignature() to throw.
        TypeSignature sig = rowType.getTypeSignature();
        assertEquals(parseTypeSignature(sig.toString()), sig);
    }

    /**
     * Regression test for issue 28141 — nested row variant.
     *
     * Mirrors the exact production type from the stack trace:
     *   row("..." varchar, PolicyHdrData row(AssignCode varchar, PolicyNumber varchar))
     * Without the fix, "..." was emitted unquoted after Jackson deserialization and
     * parseTypeSignature() threw IllegalArgumentException: Bad type signature.
     */
    @Test
    public void testDelimitedFieldNameInNestedRowPreservedAfterJsonRoundTrip()
    {
        RowType.Field dotField = new RowType.Field(Optional.of("..."), VARCHAR, true);

        RowType.Field deserialized = FIELD_CODEC.fromJson(FIELD_CODEC.toJson(dotField));
        assertTrue(deserialized.isDelimited(), "delimited flag must survive Jackson round-trip");
        RowType innerRow = RowType.from(Arrays.asList(
                RowType.field("AssignCode", VARCHAR),
                RowType.field("PolicyNumber", VARCHAR)));
        RowType outerRow = RowType.from(Arrays.asList(
                deserialized,
                RowType.field("PolicyHdrData", innerRow)));
        // Verify the full nested signature is parseable — the bug caused parseTypeSignature() to throw.
        TypeSignature sig = outerRow.getTypeSignature();
        assertEquals(parseTypeSignature(sig.toString()), sig);
    }
}
