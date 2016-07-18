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

import com.facebook.presto.spi.PrestoException;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.io.SerializedString;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;

import java.io.IOException;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.fasterxml.jackson.core.JsonFactory.Feature.CANONICALIZE_FIELD_NAMES;
import static com.fasterxml.jackson.core.JsonToken.END_ARRAY;
import static com.fasterxml.jackson.core.JsonToken.END_OBJECT;
import static com.fasterxml.jackson.core.JsonToken.FIELD_NAME;
import static com.fasterxml.jackson.core.JsonToken.START_ARRAY;
import static com.fasterxml.jackson.core.JsonToken.START_OBJECT;
import static com.fasterxml.jackson.core.JsonToken.VALUE_NULL;
import static io.airlift.slice.Slices.utf8Slice;
import static java.util.Objects.requireNonNull;

/**
 * Extracts values from JSON
 * <p/>
 * Supports the following JSON path primitives:
 * <pre>
 *    $ : Root object
 *    . or [] : Child operator
 *   [] : Subscript operator for array
 * </pre>
 * <p/>
 * Supported JSON Path Examples:
 * <pre>
 *    { "store": {
 *        "book": [
 *          { "category": "reference",
 *            "author": "Nigel Rees",
 *            "title": "Sayings of the Century",
 *            "price": 8.95,
 *            "contributors": [["Adam", "Levine"], ["Bob", "Strong"]]
 *          },
 *          { "category": "fiction",
 *            "author": "Evelyn Waugh",
 *            "title": "Sword of Honour",
 *            "price": 12.99,
 *            "isbn": "0-553-21311-3",
 *            "last_owner": null
 *          }
 *        ],
 *        "bicycle": {
 *          "color": "red",
 *          "price": 19.95
 *        }
 *      }
 *    }
 * </pre>
 * <p/>
 * With only scalar values using dot-notation of path:
 * <pre>
 *    $.store.book[0].author => Nigel Rees
 *    $.store.bicycle.price => 19.95
 *    $.store.book[0].isbn => NULL (Doesn't exist becomes java null)
 *    $.store.book[1].last_owner => NULL (json null becomes java null)
 *    $.store.book[0].contributors[0][1] => Levine
 * </pre>
 * <p/>
 * With json values using dot-notation of path:
 * <pre>
 *    $.store.book[0].author => "Nigel Rees"
 *    $.store.bicycle.price => 19.95
 *    $.store.book[0].isbn => NULL (Doesn't exist becomes java null)
 *    $.store.book[1].last_owner => null (json null becomes the string "null")
 *    $.store.book[0].contributors[0] => ["Adam", "Levine"]
 *    $.store.bicycle => {"color": "red", "price": 19.95}
 * </pre>
 * With only scalar values using bracket-notation of path:
 * <pre>
 *    $["store"]["book"][0]["author"] => Nigel Rees
 *    $["store"]["bicycle"]["price"] => 19.95
 *    $["store"]["book"][0]["isbn"] => NULL (Doesn't exist becomes java null)
 *    $["store"]["book"][1]["last_owner"] => NULL (json null becomes java null)
 *    $["store"]["book"][0]["contributors"][0][1] => Levine
 * </pre>
 * <p/>
 * With json values using bracket-notation of path:
 * <pre>
 *    $["store"]["book"][0]["author"] => "Nigel Rees"
 *    $["store"]["bicycle"]["price"] => 19.95
 *    $["store"]["book"][0]["isbn"] => NULL (Doesn't exist becomes java null)
 *    $["store"]["book"][1]["last_owner"] => null (json null becomes the string "null")
 *    $["store"]["book"][0]["contributors"][0] => ["Adam", "Levine"]
 *    $["store"]["bicycle"] => {"color": "red", "price": 19.95}
 * </pre>
 */
public final class JsonExtract
{
    private static final int ESTIMATED_JSON_OUTPUT_SIZE = 512;

    private static final JsonFactory JSON_FACTORY = new JsonFactory()
            .disable(CANONICALIZE_FIELD_NAMES);

    private JsonExtract() {}

    public static <T> T extract(Slice jsonInput, JsonExtractor<T> jsonExtractor)
    {
        requireNonNull(jsonInput, "jsonInput is null");
        try {
            try (JsonParser jsonParser = JSON_FACTORY.createParser(jsonInput.getInput())) {
                // Initialize by advancing to first token and make sure it exists
                if (jsonParser.nextToken() == null) {
                    return null;
                }

                return jsonExtractor.extract(jsonParser);
            }
        }
        catch (JsonParseException e) {
            // Return null if we failed to parse something
            return null;
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    public static <T> JsonExtractor<T> generateExtractor(String path, JsonExtractor<T> rootExtractor)
    {
        return generateExtractor(path, rootExtractor, false);
    }

    public static <T> JsonExtractor<T> generateExtractor(String path, JsonExtractor<T> rootExtractor, boolean exceptionOnOutOfBounds)
    {
        ImmutableList<String> tokens = ImmutableList.copyOf(new JsonPathTokenizer(path));

        JsonExtractor<T> jsonExtractor = rootExtractor;
        for (String token : tokens.reverse()) {
            jsonExtractor = new ObjectFieldJsonExtractor<>(token, jsonExtractor, exceptionOnOutOfBounds);
        }
        return jsonExtractor;
    }

    public interface JsonExtractor<T>
    {
        /**
         * Executes the extraction on the existing content of the JsonParser and outputs the match.
         * <p/>
         * Notes:
         * <ul>
         * <li>JsonParser must be on the FIRST token of the value to be processed when extract is called</li>
         * <li>INVARIANT: when extract() returns, the current token of the parser will be the LAST token of the value</li>
         * </ul>
         *
         * @return the value, or null if not applicable
         */
        T extract(JsonParser jsonParser)
                throws IOException;
    }

    public static class ObjectFieldJsonExtractor<T>
            implements JsonExtractor<T>
    {
        private final SerializedString fieldName;
        private final JsonExtractor<? extends T> delegate;
        private final int index;
        private final boolean exceptionOnOutOfBounds;

        public ObjectFieldJsonExtractor(String fieldName, JsonExtractor<? extends T> delegate)
        {
            this(fieldName, delegate, false);
        }

        public ObjectFieldJsonExtractor(String fieldName, JsonExtractor<? extends T> delegate, boolean exceptionOnOutOfBounds)
        {
            this.fieldName = new SerializedString(requireNonNull(fieldName, "fieldName is null"));
            this.delegate = requireNonNull(delegate, "delegate is null");
            this.exceptionOnOutOfBounds = exceptionOnOutOfBounds;
            this.index = tryParseInt(fieldName, -1);
        }

        @Override
        public T extract(JsonParser jsonParser)
                throws IOException
        {
            if (jsonParser.getCurrentToken() == START_OBJECT) {
                return processJsonObject(jsonParser);
            }

            if (jsonParser.getCurrentToken() == START_ARRAY) {
                return processJsonArray(jsonParser);
            }

            throw new JsonParseException("Expected a JSON object or array", jsonParser.getCurrentLocation());
        }

        public T processJsonObject(JsonParser jsonParser)
                throws IOException
        {
            while (!jsonParser.nextFieldName(fieldName)) {
                if (!jsonParser.hasCurrentToken()) {
                    throw new JsonParseException("Unexpected end of object", jsonParser.getCurrentLocation());
                }
                if (jsonParser.getCurrentToken() == END_OBJECT) {
                    // Unable to find matching field
                    return null;
                }
                jsonParser.skipChildren(); // Skip nested structure if currently at the start of one
            }

            jsonParser.nextToken(); // Shift to first token of the value

            return delegate.extract(jsonParser);
        }

        public T processJsonArray(JsonParser jsonParser)
                throws IOException
        {
            int currentIndex = 0;
            while (true) {
                JsonToken token = jsonParser.nextToken();
                if (token == null) {
                    throw new JsonParseException("Unexpected end of array", jsonParser.getCurrentLocation());
                }
                if (token == END_ARRAY) {
                    // Index out of bounds
                    if (exceptionOnOutOfBounds) {
                        throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Index out of bounds");
                    }
                    return null;
                }
                if (currentIndex == index) {
                    break;
                }
                currentIndex++;
                jsonParser.skipChildren(); // Skip nested structure if currently at the start of one
            }

            return delegate.extract(jsonParser);
        }
    }

    public static class ScalarValueJsonExtractor
            implements JsonExtractor<Slice>
    {
        @Override
        public Slice extract(JsonParser jsonParser)
                throws IOException
        {
            JsonToken token = jsonParser.getCurrentToken();
            if (token == null) {
                throw new JsonParseException("Unexpected end of value", jsonParser.getCurrentLocation());
            }
            if (!token.isScalarValue() || token == VALUE_NULL) {
                return null;
            }
            return utf8Slice(jsonParser.getText());
        }
    }

    public static class JsonValueJsonExtractor
            implements JsonExtractor<Slice>
    {
        @Override
        public Slice extract(JsonParser jsonParser)
                throws IOException
        {
            if (!jsonParser.hasCurrentToken()) {
                throw new JsonParseException("Unexpected end of value", jsonParser.getCurrentLocation());
            }

            DynamicSliceOutput dynamicSliceOutput = new DynamicSliceOutput(ESTIMATED_JSON_OUTPUT_SIZE);
            try (JsonGenerator jsonGenerator = JSON_FACTORY.createGenerator(dynamicSliceOutput)) {
                jsonGenerator.copyCurrentStructure(jsonParser);
            }
            return dynamicSliceOutput.slice();
        }
    }

    public static class JsonSizeExtractor
            implements JsonExtractor<Long>
    {
        @Override
        public Long extract(JsonParser jsonParser)
                throws IOException
        {
            if (!jsonParser.hasCurrentToken()) {
                throw new JsonParseException("Unexpected end of value", jsonParser.getCurrentLocation());
            }

            if (jsonParser.getCurrentToken() == START_ARRAY) {
                long length = 0;
                while (true) {
                    JsonToken token = jsonParser.nextToken();
                    if (token == null) {
                        return null;
                    }
                    if (token == END_ARRAY) {
                        return length;
                    }
                    jsonParser.skipChildren();

                    length++;
                }
            }

            if (jsonParser.getCurrentToken() == START_OBJECT) {
                long length = 0;
                while (true) {
                    JsonToken token = jsonParser.nextToken();
                    if (token == null) {
                        return null;
                    }
                    if (token == END_OBJECT) {
                        return length;
                    }

                    if (token == FIELD_NAME) {
                        length++;
                    }
                    else {
                        jsonParser.skipChildren();
                    }
                }
            }

            return 0L;
        }
    }

    private static int tryParseInt(String fieldName, int defaultValue)
    {
        int index = defaultValue;
        try {
            index = Integer.parseInt(fieldName);
        }
        catch (NumberFormatException ignored) {
        }
        return index;
    }
}
