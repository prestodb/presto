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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.io.SerializedString;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.util.Failures.checkCondition;
import static com.fasterxml.jackson.core.JsonFactory.Feature.CANONICALIZE_FIELD_NAMES;
import static com.fasterxml.jackson.core.JsonToken.END_ARRAY;
import static com.fasterxml.jackson.core.JsonToken.END_OBJECT;
import static com.fasterxml.jackson.core.JsonToken.FIELD_NAME;
import static com.fasterxml.jackson.core.JsonToken.START_ARRAY;
import static com.fasterxml.jackson.core.JsonToken.START_OBJECT;
import static com.fasterxml.jackson.core.JsonToken.VALUE_NULL;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;

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
    private static final Pattern PATH_TOKEN = Pattern.compile("(?:\\[(\\d+)])|(?:\\[(\"[^\"]*?\")])|(?:\\.([a-zA-Z_]\\w*))");
    private static final int ESTIMATED_JSON_OUTPUT_SIZE = 512;

    private static final JsonFactory JSON_FACTORY = new JsonFactory()
            .disable(CANONICALIZE_FIELD_NAMES);

    private JsonExtract() {}

    public static <T> T extract(Slice jsonInput, JsonExtractor<T> jsonExtractor)
    {
        checkNotNull(jsonInput, "jsonInput is null");
        try {
            try (JsonParser jsonParser = JSON_FACTORY.createJsonParser(jsonInput.getInput())) {
                // Initialize by advancing to first token and make sure it exists
                if (jsonParser.nextToken() == null) {
                    throw new JsonParseException("Missing starting token", jsonParser.getCurrentLocation());
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

    @VisibleForTesting
    public static ImmutableList<Object> tokenizePath(String path)
    {
        checkCondition(!isNullOrEmpty(path), INVALID_FUNCTION_ARGUMENT, "Invalid JSON path: '%s'", path);
        checkCondition(path.charAt(0) == '$', INVALID_FUNCTION_ARGUMENT, "JSON path must start with '$': '%s'", path);

        if (path.length() == 1) {
            return ImmutableList.of();
        }

        ImmutableList.Builder<Object> tokens = ImmutableList.builder();
        Matcher matcher = PATH_TOKEN.matcher(path).region(1, path.length());
        int lastMatchEnd = 1;
        while (matcher.find()) {
            checkCondition(lastMatchEnd == matcher.start(), INVALID_FUNCTION_ARGUMENT, "Invalid JSON path: '%s'", path);
            lastMatchEnd = matcher.end();

            // token is in either the first, second, or third group
            String token = matcher.group(1);
            if (token == null) {
                token = matcher.group(2);
            }
            if (token == null) {
                token = matcher.group(3);
            }

            if (Character.isDigit(token.charAt(0))) {
                tokens.add(Integer.parseInt(token));
            }
            else {
                // strip quotes
                if (token.charAt(0) == '"') {
                    token = token.substring(1, token.length() - 1);
                }
                checkCondition(token.indexOf('"') < 0, INVALID_FUNCTION_ARGUMENT, "JSON path token contains a quote: '%s'", path);
                tokens.add(token);
            }
        }
        // part of the stream did not match
        checkCondition(lastMatchEnd == path.length(), INVALID_FUNCTION_ARGUMENT, "Invalid JSON path: '%s'", path);
        return tokens.build();
    }

    public static <T> JsonExtractor<T> generateExtractor(String path, JsonExtractor<T> rootExtractor)
    {
        ImmutableList<Object> tokens = tokenizePath(path);

        JsonExtractor<T> jsonExtractor = rootExtractor;
        for (Object token : tokens.reverse()) {
            if (token instanceof String) {
                jsonExtractor = new ObjectFieldJsonExtractor<>((String) token, jsonExtractor);
            }
            else if (token instanceof Integer) {
                jsonExtractor = new ArrayElementJsonExtractor<>((Integer) token, jsonExtractor);
            }
            else {
                throw new IllegalStateException("Unsupported JSON path token type " + token.getClass().getName());
            }
        }
        return jsonExtractor;
    }

    public interface JsonExtractor<T>
    {
        /**
         * Executes the extraction on the existing content of the JasonParser and outputs the match.
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

        public ObjectFieldJsonExtractor(String fieldName, JsonExtractor<? extends T> delegate)
        {
            this.fieldName = new SerializedString(checkNotNull(fieldName, "fieldName is null"));
            this.delegate = checkNotNull(delegate, "delegate is null");
        }

        @Override
        public T extract(JsonParser jsonParser)
                throws IOException
        {
            if (jsonParser.getCurrentToken() != START_OBJECT) {
                throw new JsonParseException("Expected a Json object", jsonParser.getCurrentLocation());
            }

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
    }

    public static class ArrayElementJsonExtractor<T>
            implements JsonExtractor<T>
    {
        private final int index;
        private final JsonExtractor<? extends T> delegate;

        public ArrayElementJsonExtractor(int index, JsonExtractor<? extends T> delegate)
        {
            checkArgument(index >= 0, "index must be greater than or equal to zero: %s", index);
            checkNotNull(delegate, "delegate is null");
            this.index = index;
            this.delegate = delegate;
        }

        @Override
        public T extract(JsonParser jsonParser)
                throws IOException
        {
            if (jsonParser.getCurrentToken() != START_ARRAY) {
                throw new JsonParseException("Expected a Json array", jsonParser.getCurrentLocation());
            }

            int currentIndex = 0;
            while (true) {
                JsonToken token = jsonParser.nextToken();
                if (token == null) {
                    throw new JsonParseException("Unexpected end of array", jsonParser.getCurrentLocation());
                }
                if (token == END_ARRAY) {
                    // Index out of bounds
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
            return Slices.wrappedBuffer(jsonParser.getText().getBytes(Charsets.UTF_8));
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
            try (JsonGenerator jsonGenerator = JSON_FACTORY.createJsonGenerator(dynamicSliceOutput)) {
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
            else if (jsonParser.getCurrentToken() == START_OBJECT) {
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
            else {
                return 0L;
            }
        }
    }
}
