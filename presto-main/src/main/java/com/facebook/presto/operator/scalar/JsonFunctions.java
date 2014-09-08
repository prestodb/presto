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
import com.google.common.base.Charsets;
import com.google.common.primitives.Doubles;
import io.airlift.slice.Slice;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

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
    @SqlType(JsonPathType.NAME)
    public static JsonPath castToJsonPath(@SqlType(VarcharType.NAME) Slice pattern)
    {
        return new JsonPath(pattern.toString(UTF_8));
    }

    @Nullable
    @ScalarFunction
    @SqlType(BigintType.NAME)
    public static Long jsonArrayLength(@SqlType(VarcharType.NAME) Slice json)
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
    @SqlType(BooleanType.NAME)
    public static Boolean jsonArrayContains(@SqlType(VarcharType.NAME) Slice json, @SqlType(BooleanType.NAME) boolean value)
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
    @SqlType(BooleanType.NAME)
    public static Boolean jsonArrayContains(@SqlType(VarcharType.NAME) Slice json, @SqlType(BigintType.NAME) long value)
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
    @SqlType(BooleanType.NAME)
    public static Boolean jsonArrayContains(@SqlType(VarcharType.NAME) Slice json, @SqlType(DoubleType.NAME) double value)
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
    @SqlType(BooleanType.NAME)
    public static Boolean jsonArrayContains(@SqlType(VarcharType.NAME) Slice json, @SqlType(VarcharType.NAME) Slice value)
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
    @SqlType(VarcharType.NAME)
    public static Slice jsonArrayGet(@SqlType(VarcharType.NAME) Slice json, @SqlType(BigintType.NAME) long index)
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
    @SqlType(VarcharType.NAME)
    public static Slice jsonExtractScalar(@SqlType(VarcharType.NAME) Slice json, @SqlType(JsonPathType.NAME) JsonPath jsonPath)
    {
        return JsonExtract.extract(json, jsonPath.getScalarExtractor());
    }

    @ScalarFunction
    @Nullable
    @SqlType(VarcharType.NAME)
    public static Slice jsonExtract(@SqlType(VarcharType.NAME) Slice json, @SqlType(JsonPathType.NAME) JsonPath jsonPath)
    {
        return JsonExtract.extract(json, jsonPath.getObjectExtractor());
    }

    @ScalarFunction
    @Nullable
    @SqlType(BigintType.NAME)
    public static Long jsonSize(@SqlType(VarcharType.NAME) Slice json, @SqlType(JsonPathType.NAME) JsonPath jsonPath)
    {
        return JsonExtract.extract(json, jsonPath.getSizeExtractor());
    }
}
