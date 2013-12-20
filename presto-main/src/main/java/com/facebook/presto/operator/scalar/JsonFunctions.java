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

import com.facebook.presto.byteCode.ByteCodeNode;
import com.facebook.presto.byteCode.instruction.Constant;
import com.facebook.presto.operator.scalar.JsonExtract.JsonExtractCache;
import com.facebook.presto.operator.scalar.JsonExtract.JsonExtractor;
import com.facebook.presto.sql.gen.DefaultFunctionBinder;
import com.facebook.presto.sql.gen.FunctionBinder;
import com.facebook.presto.sql.gen.FunctionBinding;
import com.facebook.presto.sql.gen.TypedByteCodeNode;
import com.facebook.presto.util.ThreadLocalCache;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Doubles;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import javax.annotation.Nullable;

import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import static com.facebook.presto.operator.scalar.JsonExtract.generateExtractor;
import static com.fasterxml.jackson.core.JsonFactory.Feature.CANONICALIZE_FIELD_NAMES;
import static com.fasterxml.jackson.core.JsonParser.NumberType;
import static com.fasterxml.jackson.core.JsonToken.END_ARRAY;
import static com.fasterxml.jackson.core.JsonToken.START_ARRAY;
import static com.fasterxml.jackson.core.JsonToken.VALUE_FALSE;
import static com.fasterxml.jackson.core.JsonToken.VALUE_NUMBER_FLOAT;
import static com.fasterxml.jackson.core.JsonToken.VALUE_NUMBER_INT;
import static com.fasterxml.jackson.core.JsonToken.VALUE_STRING;
import static com.fasterxml.jackson.core.JsonToken.VALUE_TRUE;
import static java.lang.invoke.MethodHandles.lookup;
import static java.lang.invoke.MethodType.methodType;

public final class JsonFunctions
{
    private static final String JSON_EXTRACT_SCALAR_FUNCTION_NAME = "json_extract_scalar";
    private static final String JSON_EXTRACT_FUNCTION_NAME = "json_extract";

    private static final JsonFactory JSON_FACTORY = new JsonFactory()
            .disable(CANONICALIZE_FIELD_NAMES);

    private JsonFunctions() {}

    @Nullable
    @ScalarFunction
    public static Long jsonArrayLength(Slice json)
    {
        try (JsonParser parser = JSON_FACTORY.createJsonParser(json.getInput())) {
            if (parser.nextToken() != START_ARRAY) {
                return null;
            }

            long length = 0;
            while (true) {
                JsonToken token = parser.nextToken();
                if (token == null) {
                    return null;
                }
                if (token == END_ARRAY) {
                    return length;
                }
                parser.skipChildren();

                length++;
            }
        }
        catch (IOException e) {
            return null;
        }
    }

    @Nullable
    @ScalarFunction
    public static Boolean jsonArrayContains(Slice json, boolean value)
    {
        try (JsonParser parser = JSON_FACTORY.createJsonParser(json.getInput())) {
            if (parser.nextToken() != START_ARRAY) {
                return null;
            }

            while (true) {
                JsonToken token = parser.nextToken();
                if (token == null) {
                    return null;
                }
                if (token == END_ARRAY) {
                    return false;
                }
                parser.skipChildren();

                if (((token == VALUE_TRUE) && value) ||
                        ((token == VALUE_FALSE) && (!value))) {
                    return true;
                }
            }
        }
        catch (IOException e) {
            return null;
        }
    }

    @Nullable
    @ScalarFunction
    public static Boolean jsonArrayContains(Slice json, long value)
    {
        try (JsonParser parser = JSON_FACTORY.createJsonParser(json.getInput())) {
            if (parser.nextToken() != START_ARRAY) {
                return null;
            }

            while (true) {
                JsonToken token = parser.nextToken();
                if (token == null) {
                    return null;
                }
                if (token == END_ARRAY) {
                    return false;
                }
                parser.skipChildren();

                if ((token == VALUE_NUMBER_INT) &&
                        ((parser.getNumberType() == NumberType.INT) || (parser.getNumberType() == NumberType.LONG)) &&
                        (parser.getLongValue() == value)) {
                    return true;
                }
            }
        }
        catch (IOException e) {
            return null;
        }
    }

    @Nullable
    @ScalarFunction
    public static Boolean jsonArrayContains(Slice json, double value)
    {
        if (!Doubles.isFinite(value)) {
            return false;
        }

        try (JsonParser parser = JSON_FACTORY.createJsonParser(json.getInput())) {
            if (parser.nextToken() != START_ARRAY) {
                return null;
            }

            while (true) {
                JsonToken token = parser.nextToken();
                if (token == null) {
                    return null;
                }
                if (token == END_ARRAY) {
                    return false;
                }
                parser.skipChildren();

                // noinspection FloatingPointEquality
                if ((token == VALUE_NUMBER_FLOAT) && (parser.getDoubleValue() == value) &&
                        (Doubles.isFinite(parser.getDoubleValue()))) {
                    return true;
                }
            }
        }
        catch (IOException e) {
            return null;
        }
    }

    @Nullable
    @ScalarFunction
    public static Boolean jsonArrayContains(Slice json, Slice value)
    {
        String valueString = value.toString(Charsets.UTF_8);

        try (JsonParser parser = JSON_FACTORY.createJsonParser(json.getInput())) {
            if (parser.nextToken() != START_ARRAY) {
                return null;
            }

            while (true) {
                JsonToken token = parser.nextToken();
                if (token == null) {
                    return null;
                }
                if (token == END_ARRAY) {
                    return false;
                }
                parser.skipChildren();

                if (token == VALUE_STRING && valueString.equals(parser.getValueAsString())) {
                    return true;
                }
            }
        }
        catch (IOException e) {
            return null;
        }
    }

    @Nullable
    @ScalarFunction
    public static Slice jsonArrayGet(Slice json, long index)
    {
        try (JsonParser parser = JSON_FACTORY.createJsonParser(json.getInput())) {
            if (parser.nextToken() != START_ARRAY) {
                return null;
            }

            List<String> tokens = null;
            if (index < 0) {
                tokens = new LinkedList<String>();
            }

            long count = 0;
            while (true) {
                JsonToken token = parser.nextToken();
                if (token == null) {
                    return null;
                }
                if (token == END_ARRAY) {
                    if (tokens != null && count >= index * -1) {
                        return Slices.utf8Slice(tokens.get(0));
                    }

                    return null;
                }
                parser.skipChildren();

                if (count == index) {
                    if (parser.getValueAsString() == null) {
                        return null;
                    }
                    return Slices.utf8Slice(parser.getValueAsString());

                }

                if (tokens != null) {
                    tokens.add(parser.getValueAsString());

                    if (count >= index * -1) {
                        tokens.remove(0);
                    }
                }

                count++;
            }
        }
        catch (IOException e) {
            return null;
        }
    }

    @ScalarFunction(value = JSON_EXTRACT_SCALAR_FUNCTION_NAME, functionBinder = JsonFunctionBinder.class)
    public static Slice jsonExtractScalar(Slice json, Slice jsonPath)
    {
        try {
            return JsonExtract.extractScalar(json, jsonPath);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @ScalarFunction(value = JSON_EXTRACT_FUNCTION_NAME, functionBinder = JsonFunctionBinder.class)
    public static Slice jsonExtract(Slice json, Slice jsonPath)
    {
        try {
            return JsonExtract.extractJson(json, jsonPath);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    public static class JsonFunctionBinder
            implements FunctionBinder
    {
        private static final MethodHandle constantJsonExtract;
        private static final MethodHandle dynamicJsonExtract;

        static {
            try {
                constantJsonExtract = lookup().findStatic(JsonExtract.class, "extract", methodType(Slice.class, Slice.class, JsonExtractor.class));
                dynamicJsonExtract = lookup().findStatic(JsonExtract.class, "extract", methodType(Slice.class, ThreadLocalCache.class, Slice.class, Slice.class));
            }
            catch (ReflectiveOperationException e) {
                throw Throwables.propagate(e);
            }
        }

        @Override
        public FunctionBinding bindFunction(long bindingId, String name, ByteCodeNode getSessionByteCode, List<TypedByteCodeNode> arguments)
        {
            TypedByteCodeNode patternNode = arguments.get(1);

            MethodHandle methodHandle;
            if (patternNode.getNode() instanceof Constant) {
                Slice patternSlice = (Slice) ((Constant) patternNode.getNode()).getValue();
                String pattern = patternSlice.toString(Charsets.UTF_8);

                JsonExtractor jsonExtractor;
                switch (name) {
                    case JSON_EXTRACT_SCALAR_FUNCTION_NAME:
                        jsonExtractor = generateExtractor(pattern, true);
                        break;
                    case JSON_EXTRACT_FUNCTION_NAME:
                        jsonExtractor = generateExtractor(pattern, false);
                        break;
                    default:
                        throw new IllegalArgumentException("Unsupported method " + name);
                }

                methodHandle = MethodHandles.insertArguments(constantJsonExtract, 1, jsonExtractor);

                // remove the pattern argument
                arguments = new ArrayList<>(arguments);
                arguments.remove(1);
                arguments = ImmutableList.copyOf(arguments);
            }
            else {
                ThreadLocalCache<Slice, JsonExtractor> cache;
                switch (name) {
                    case JSON_EXTRACT_SCALAR_FUNCTION_NAME:
                        cache = new JsonExtractCache(20, true);
                        break;
                    case JSON_EXTRACT_FUNCTION_NAME:
                        cache = new JsonExtractCache(20, false);
                        break;
                    default:
                        throw new IllegalArgumentException("Unsupported method " + name);
                }

                methodHandle = dynamicJsonExtract.bindTo(cache);
            }

            return DefaultFunctionBinder.bindConstantArguments(bindingId, name, getSessionByteCode, arguments, methodHandle, true);
        }
    }
}
