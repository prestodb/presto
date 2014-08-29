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
package com.facebook.presto.operator.scalar;

import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.ParametricOperator;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.ArrayType;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.metadata.OperatorType.SUBSCRIPT;
import static com.facebook.presto.metadata.Signature.typeParameter;
import static com.facebook.presto.util.Failures.checkCondition;
import static com.fasterxml.jackson.core.JsonFactory.Feature.CANONICALIZE_FIELD_NAMES;
import static com.fasterxml.jackson.core.JsonToken.END_ARRAY;
import static com.fasterxml.jackson.core.JsonToken.START_ARRAY;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.invoke.MethodHandles.lookup;

public class ArraySubscriptOperator
        extends ParametricOperator
{
    public static final ArraySubscriptOperator ARRAY_SUBSCRIPT = new ArraySubscriptOperator();
    private static final JsonFactory JSON_FACTORY = new JsonFactory().disable(CANONICALIZE_FIELD_NAMES);
    private static final Map<Class<?>, MethodHandle> METHOD_HANDLES;

    static {
        ImmutableMap.Builder<Class<?>, MethodHandle> builder = ImmutableMap.builder();
        try {
            builder.put(boolean.class, lookup().unreflect(ArraySubscriptOperator.class.getMethod("booleanElementAccessor", Slice.class, long.class)));
            builder.put(long.class, lookup().unreflect(ArraySubscriptOperator.class.getMethod("longElementAccessor", Slice.class, long.class)));
            builder.put(double.class, lookup().unreflect(ArraySubscriptOperator.class.getMethod("doubleElementAccessor", Slice.class, long.class)));
            builder.put(Slice.class, lookup().unreflect(ArraySubscriptOperator.class.getMethod("sliceElementAccessor", Slice.class, long.class)));
        }
        catch (ReflectiveOperationException e) {
            throw Throwables.propagate(e);
        }
        METHOD_HANDLES = builder.build();
    }

    protected ArraySubscriptOperator()
    {
        super(SUBSCRIPT, ImmutableList.of(typeParameter("E")), "E", ImmutableList.of("array<E>", "bigint"));
    }

    @Override
    public String getDescription()
    {
        return "Array subscript";
    }

    @Override
    public FunctionInfo specialize(List<? extends Type> types)
    {
        checkArgument(types.size() == 2, "Expected two types, got %s", types);
        checkArgument(types.get(0) instanceof ArrayType, "Expected an array type as the first type");
        checkArgument(types.get(1) instanceof BigintType, "Expected bigint type as second type");
        ArrayType arrayType = (ArrayType) types.get(0);
        Type elementType = arrayType.getElementType();

        MethodHandle methodHandle = METHOD_HANDLES.get(elementType.getJavaType());
        checkNotNull(methodHandle, "methodHandle is null");
        return new FunctionInfo(Signature.internalOperator(SUBSCRIPT.name(), elementType.getName(), arrayType.getName(), StandardTypes.BIGINT), "Array subscript", true, methodHandle, true, true, ImmutableList.of(false, false));
    }

    @Override
    public FunctionInfo specialize(Type returnType, List<? extends Type> types)
    {
        return specialize(types);
    }

    public static Boolean booleanElementAccessor(Slice array, long index)
    {
        checkCondition(index > 0, StandardErrorCode.INVALID_FUNCTION_ARGUMENT, "Array index must be greater than zero. Got %d", index);
        try (JsonParser jsonParser = JSON_FACTORY.createJsonParser(array.getInput())) {
            checkState(jsonParser.nextToken() == START_ARRAY, "Expected start of array in input: \"%s\"", array.toStringUtf8());
            jsonParser.nextToken();
            seekTo(jsonParser, index);
            if (jsonParser.getCurrentToken() == JsonToken.VALUE_NULL) {
                return null;
            }
            return jsonParser.getBooleanValue();
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    public static Long longElementAccessor(Slice array, long index)
    {
        checkCondition(index > 0, StandardErrorCode.INVALID_FUNCTION_ARGUMENT, "Array index must be greater than zero. Got %d", index);
        try (JsonParser jsonParser = JSON_FACTORY.createJsonParser(array.getInput())) {
            checkState(jsonParser.nextToken() == START_ARRAY, "Expected start of array in input: \"%s\"", array.toStringUtf8());
            jsonParser.nextToken();
            seekTo(jsonParser, index);
            if (jsonParser.getCurrentToken() == JsonToken.VALUE_NULL) {
                return null;
            }
            return jsonParser.getLongValue();
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    public static Double doubleElementAccessor(Slice array, long index)
    {
        checkCondition(index > 0, StandardErrorCode.INVALID_FUNCTION_ARGUMENT, "Array index must be greater than zero. Got %d", index);
        try (JsonParser jsonParser = JSON_FACTORY.createJsonParser(array.getInput())) {
            checkState(jsonParser.nextToken() == START_ARRAY, "Expected start of array in input: \"%s\"", array.toStringUtf8());
            jsonParser.nextToken();
            seekTo(jsonParser, index);
            if (jsonParser.getCurrentToken() == JsonToken.VALUE_NULL) {
                return null;
            }
            return jsonParser.getDoubleValue();
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    public static Slice sliceElementAccessor(Slice array, long index)
    {
        checkCondition(index > 0, StandardErrorCode.INVALID_FUNCTION_ARGUMENT, "Array index must be greater than zero. Got %d", index);
        try (JsonParser jsonParser = JSON_FACTORY.createJsonParser(array.getInput())) {
            checkState(jsonParser.nextToken() == START_ARRAY, "Expected start of array in input: \"%s\"", array.toStringUtf8());
            jsonParser.nextToken();
            seekTo(jsonParser, index);
            String stringValue = jsonParser.getValueAsString();
            if (stringValue == null) {
                return null;
            }
            return Slices.utf8Slice(stringValue);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    private static void seekTo(JsonParser jsonParser, long position)
            throws IOException
    {
        // Arrays are 1-indexed
        for (long i = 1; i < position; i++) {
            JsonToken token = jsonParser.nextToken();
            if (token == null || token == END_ARRAY) {
                throw new PrestoException(StandardErrorCode.INVALID_FUNCTION_ARGUMENT.toErrorCode(), "Index out of bounds");
            }
        }
    }
}
