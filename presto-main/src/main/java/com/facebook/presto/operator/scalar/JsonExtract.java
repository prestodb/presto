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

import com.facebook.presto.util.ThreadLocalCache;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.io.SerializedString;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.fasterxml.jackson.core.JsonFactory.Feature.CANONICALIZE_FIELD_NAMES;
import static com.fasterxml.jackson.core.JsonToken.END_ARRAY;
import static com.fasterxml.jackson.core.JsonToken.END_OBJECT;
import static com.fasterxml.jackson.core.JsonToken.START_ARRAY;
import static com.fasterxml.jackson.core.JsonToken.START_OBJECT;
import static com.fasterxml.jackson.core.JsonToken.VALUE_NULL;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Extracts values from JSON
 * <p/>
 * Supports the following JSON path primitives:
 * <pre>
 *    $ : Root object
 *    . : Child operator
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
 * With only scalar values:
 * <pre>
 *    $.store.book[0].author => Nigel Rees
 *    $.store.bicycle.price => 19.95
 *    $.store.book[0].isbn => NULL (Doesn't exist becomes java null)
 *    $.store.book[1].last_owner => NULL (json null becomes java null)
 *    $.store.book[0].contributors[0][1] => Levine
 * </pre>
 * <p/>
 * With json values:
 * <pre>
 *    $.store.book[0].author => "Nigel Rees"
 *    $.store.bicycle.price => 19.95
 *    $.store.book[0].isbn => NULL (Doesn't exist becomes java null)
 *    $.store.book[1].last_owner => null (json null becomes the string "null")
 *    $.store.book[0].contributors[0] => ["Adam", "Levine"]
 *    $.store.bicycle => {"color": "red", "price": 19.95}
 * </pre>
 */
public final class JsonExtract
{
    private static final Pattern EXPECTED_PATH = Pattern.compile("\\$(\\[\\d+\\])*(\\.[^@\\.\\[\\]\\$\\*]+(\\[\\d+\\])*)*");
    private static final int ESTIMATED_JSON_OUTPUT_SIZE = 512;

    private static final List<StringReplacer> PATH_STRING_REPLACERS = ImmutableList.of(
            new StringReplacer("[", ".["),
            new StringReplacer("]", "")
    );

    private static final Splitter DOT_SPLITTER = Splitter.on(".").trimResults();
    private static final JsonFactory JSON_FACTORY = new JsonFactory()
            .disable(CANONICALIZE_FIELD_NAMES);

    // Stand-in caches for compiled JSON paths until we have something more formalized
    private static final JsonExtractCache SCALAR_CACHE = new JsonExtractCache(20, true);
    private static final JsonExtractCache JSON_CACHE = new JsonExtractCache(20, false);

    private JsonExtract() {}

    /**
     * Main scalar extraction entry point
     *
     * @param jsonInput - Slice representation of a JSON object to inspect
     * @param jsonPath - Slice representation of the extraction path
     * @return extracted scalar value as Slice, or NULL on mismatch
     * @throws JsonParseException - jsonInput is malformed
     * @throws IOException
     */
    public static Slice extractScalar(@Nullable Slice jsonInput, Slice jsonPath)
            throws IOException
    {
        return extract(SCALAR_CACHE, jsonInput, jsonPath);
    }

    /**
     * Main json extraction entry point
     *
     * @param jsonInput - Slice representation of a JSON object to inspect
     * @param jsonPath - Slice representation of the extraction path
     * @return extracted json value as Slice, or NULL on mismatch
     * @throws JsonParseException - jsonInput is malformed
     * @throws IOException
     */
    public static Slice extractJson(@Nullable Slice jsonInput, Slice jsonPath)
            throws IOException
    {
        return extract(JSON_CACHE, jsonInput, jsonPath);
    }

    public static Slice extract(ThreadLocalCache<Slice, JsonExtractor> cache, @Nullable Slice jsonInput, Slice jsonPath)
            throws IOException
    {
        checkNotNull(jsonPath, "jsonPath is null");
        if (jsonInput == null) {
            return null;
        }

        try {
            return extractInternal(jsonInput, cache.get(jsonPath));
        }
        catch (JsonParseException e) {
            // Return null if we failed to parse something
            return null;
        }
    }

    public static Slice extract(Slice jsonInput, JsonExtractor jsonExtractor)
            throws IOException
    {
        try {
            return extractInternal(jsonInput, jsonExtractor);
        }
        catch (JsonParseException e) {
            // Return null if we failed to parse something
            return null;
        }
    }

    @VisibleForTesting
    static Slice extractInternal(Slice jsonInput, JsonExtractor jsonExtractor)
            throws IOException
    {
        checkNotNull(jsonInput, "jsonInput is null");
        try (JsonParser jsonParser = JSON_FACTORY.createJsonParser(jsonInput.getInput())) {
            // Initialize by advancing to first token and make sure it exists
            if (jsonParser.nextToken() == null) {
                throw new JsonParseException("Missing starting token", jsonParser.getCurrentLocation());
            }

            return jsonExtractor.extract(jsonParser);
        }
    }

    private static Iterable<String> tokenizePath(String path)
    {
        checkArgument(EXPECTED_PATH.matcher(path).matches(), "Invalid/unsupported JSON path: '%s'", path);
        // This performs the following transformation:
        // $.blah[0].fuu[1][2].bar => $.blah.[0.fuu.[1.[2.bar
        for (StringReplacer replacer : PATH_STRING_REPLACERS) {
            path = replacer.replace(path);
        }
        return DOT_SPLITTER.split(path);
    }

    public static JsonExtractor generateExtractor(String path, boolean scalarValue)
    {
        Iterator<String> iterator = tokenizePath(path).iterator();
        checkArgument(iterator.hasNext() && iterator.next().equals("$"), "JSON path must begin with root: '$'");
        return generateExtractor(iterator, scalarValue);
    }

    private static JsonExtractor generateExtractor(Iterator<String> filters, boolean scalarValue)
    {
        if (!filters.hasNext()) {
            return scalarValue ? new ScalarValueJsonExtractor() : new JsonValueJsonExtractor();
        }

        String filter = filters.next();
        if (filter.startsWith("[")) {
            int index = Integer.parseInt(filter.substring(1).trim());
            return new ArrayElementJsonExtractor(index, generateExtractor(filters, scalarValue));
        }
        else {
            return new ObjectFieldJsonExtractor(filter, generateExtractor(filters, scalarValue));
        }
    }

    public interface JsonExtractor
    {
        /**
         * Executes the extraction on the existing content of the JasonParser and outputs the value as a Slice.
         * <p/>
         * Notes:
         * <ul>
         * <li>JsonParser must be on the FIRST token of the value to be processed when extract is called</li>
         * <li>INVARIANT: when extract() returns, the current token of the parser will be the LAST token of the value</li>
         * </ul>
         *
         * @return Slice of the value, or null if not applicable
         */
        Slice extract(JsonParser jsonParser)
                throws IOException;
    }

    public static class ObjectFieldJsonExtractor
            implements JsonExtractor
    {
        private final SerializedString fieldName;
        private final JsonExtractor delegate;

        public ObjectFieldJsonExtractor(String fieldName, JsonExtractor delegate)
        {
            this.fieldName = new SerializedString(checkNotNull(fieldName, "fieldName is null"));
            this.delegate = checkNotNull(delegate, "delegate is null");
        }

        @Override
        public Slice extract(JsonParser jsonParser)
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

    public static class ArrayElementJsonExtractor
            implements JsonExtractor
    {
        private final int index;
        private final JsonExtractor delegate;

        public ArrayElementJsonExtractor(int index, JsonExtractor delegate)
        {
            checkArgument(index >= 0, "index must be greater than or equal to zero: %s", index);
            checkNotNull(delegate, "delegate is null");
            this.index = index;
            this.delegate = delegate;
        }

        @Override
        public Slice extract(JsonParser jsonParser)
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
            implements JsonExtractor
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
            implements JsonExtractor
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

    private static class StringReplacer
    {
        private final Pattern pattern;
        private final String replacement;

        private StringReplacer(String original, String replacement)
        {
            this.pattern = Pattern.compile(original, Pattern.LITERAL);
            this.replacement = Matcher.quoteReplacement(replacement);
        }

        public String replace(String target)
        {
            return pattern.matcher(target).replaceAll(replacement);
        }
    }

    public static class JsonExtractCache
            extends ThreadLocalCache<Slice, JsonExtractor>
    {
        private final boolean isScalarValue;

        public JsonExtractCache(int sizePerThread, boolean isScalarValue)
        {
            super(sizePerThread);
            this.isScalarValue = isScalarValue;
        }

        @Override
        protected JsonExtractor load(Slice jsonPath)
        {
            return generateExtractor(jsonPath.toString(Charsets.UTF_8), isScalarValue);
        }
    }
}
