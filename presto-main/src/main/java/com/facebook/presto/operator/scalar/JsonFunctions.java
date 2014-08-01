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

import com.facebook.presto.metadata.OperatorType;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.type.JsonPathType;
import com.facebook.presto.type.SqlType;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.primitives.Doubles;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.fasterxml.jackson.core.JsonFactory.Feature.CANONICALIZE_FIELD_NAMES;
import static com.fasterxml.jackson.core.JsonParser.NumberType;
import static com.fasterxml.jackson.core.JsonToken.END_ARRAY;
import static com.fasterxml.jackson.core.JsonToken.START_ARRAY;
import static com.fasterxml.jackson.core.JsonToken.VALUE_FALSE;
import static com.fasterxml.jackson.core.JsonToken.VALUE_NUMBER_FLOAT;
import static com.fasterxml.jackson.core.JsonToken.VALUE_NUMBER_INT;
import static com.fasterxml.jackson.core.JsonToken.VALUE_STRING;
import static com.fasterxml.jackson.core.JsonToken.VALUE_TRUE;
import static io.airlift.slice.Slices.utf8Slice;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class JsonFunctions
{
    private static final JsonFactory JSON_FACTORY = new JsonFactory()
            .disable(CANONICALIZE_FIELD_NAMES);

    private JsonFunctions() {}

    @ScalarOperator(OperatorType.CAST)
    @SqlType(JsonPathType.class)
    public static JsonPath castToJsonPath(@SqlType(VarcharType.class) Slice pattern)
    {
        return new JsonPath(pattern.toString(UTF_8));
    }

    @Nullable
    @ScalarFunction
    @SqlType(BigintType.class)
    public static Long jsonArrayLength(@SqlType(VarcharType.class) Slice json)
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
    @SqlType(BooleanType.class)
    public static Boolean jsonArrayContains(@SqlType(VarcharType.class) Slice json, @SqlType(BooleanType.class) boolean value)
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
    @SqlType(BooleanType.class)
    public static Boolean jsonArrayContains(@SqlType(VarcharType.class) Slice json, @SqlType(BigintType.class) long value)
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
    @SqlType(BooleanType.class)
    public static Boolean jsonArrayContains(@SqlType(VarcharType.class) Slice json, @SqlType(DoubleType.class) double value)
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
    @SqlType(BooleanType.class)
    public static Boolean jsonArrayContains(@SqlType(VarcharType.class) Slice json, @SqlType(VarcharType.class) Slice value)
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
    @SqlType(VarcharType.class)
    public static Slice jsonArrayGet(@SqlType(VarcharType.class) Slice json, @SqlType(BigintType.class) long index)
    {
        try (JsonParser parser = JSON_FACTORY.createJsonParser(json.getInput())) {
            if (parser.nextToken() != START_ARRAY) {
                return null;
            }

            List<String> tokens = null;
            if (index < 0) {
                tokens = new LinkedList<>();
            }

            long count = 0;
            while (true) {
                JsonToken token = parser.nextToken();
                if (token == null) {
                    return null;
                }
                if (token == END_ARRAY) {
                    if (tokens != null && count >= index * -1) {
                        return utf8Slice(tokens.get(0));
                    }

                    return null;
                }
                parser.skipChildren();

                if (count == index) {
                    if (parser.getValueAsString() == null) {
                        return null;
                    }
                    return utf8Slice(parser.getValueAsString());
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

    @ScalarFunction
    @Nullable
    @SqlType(VarcharType.class)
    public static Slice jsonExtractScalar(@SqlType(VarcharType.class) Slice json, @SqlType(JsonPathType.class) JsonPath jsonPath)
    {
        return JsonExtract.extract(json, jsonPath.getScalarExtractor());
    }

    @ScalarFunction
    @Nullable
    @SqlType(VarcharType.class)
    public static Slice jsonExtract(@SqlType(VarcharType.class) Slice json, @SqlType(JsonPathType.class) JsonPath jsonPath)
    {
        return JsonExtract.extract(json, jsonPath.getObjectExtractor());
    }

    @ScalarFunction
    @Nullable
    @SqlType(BigintType.class)
    public static Long jsonSize(@SqlType(VarcharType.class) Slice json, @SqlType(JsonPathType.class) JsonPath jsonPath)
    {
        return JsonExtract.extract(json, jsonPath.getSizeExtractor());
    }

    /**
     * Until presto supports table generating functions, this is a workaround to support extracting multiple fields from a json string
     * Presto also doesn't support variable length arguments so keys will be a string representation of an array of keys to extract, e.g., ["a", "b", "c.d"]
     */
    @ScalarFunction()
    @SqlType(VarcharType.class)
    public static Slice jsonExtractMultiple(@SqlType(VarcharType.class) Slice json, @SqlType(VarcharType.class) Slice keys)
    {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode newNode = mapper.createObjectNode();
        try {
            JsonNode rootNode = mapper.readTree(json.getInput());
            String [] keys2Extract = ArrayStringHelper.toStringArray(keys.toString(Charsets.UTF_8));
            for (String key : keys2Extract) {
                    JsonNode node = rootNode.get(key);
                    if (node != null) {
                        newNode.put(key, node);
                    }
            }
        }
        catch (IOException e) {
            return null;
        }

        if (newNode.size() == 0) {
            return null;
        }
        else {
            return Slices.wrappedBuffer(newNode.toString().getBytes(Charsets.UTF_8));
        }
    }

    @VisibleForTesting
    public static class ArrayStringHelper
    {
        /**
         * Input string has brackets and double quotes around individual array elements ["a", "b", "c.d"]
         *
         * @param stringRepr
         * @return a string array created from the given stringRepr
         */
        public static String[] toStringArray(String stringRepr)
        {
            int len = stringRepr.length();
            if (stringRepr.charAt(0) != '[' || stringRepr.charAt(len - 1) != ']') {
                throw new IllegalArgumentException("Input string should have opening and closing brackets, e.g., [\"a\", \"b\", \"c.d\"]");
            }
            stringRepr = stringRepr.substring(1, len - 1);

            List<String> matches = new ArrayList<String>();
            //first split considering anything in double quotes
            Pattern regex = Pattern.compile("\\s\"']+|\"[^\"]*\"");
            Matcher regexMatcher = regex.matcher(stringRepr);
            while (regexMatcher.find()) {
                String stripped = regexMatcher.group().replaceAll("^\"|\"$", ""); //get rid of double quotes
                matches.add(stripped);
            }
            return matches.toArray(new String[matches.size()]);
        }
    }
}
