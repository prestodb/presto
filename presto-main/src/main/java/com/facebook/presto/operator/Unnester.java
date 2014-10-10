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
package com.facebook.presto.operator;

import com.facebook.presto.spi.PageBuilder;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.google.common.base.Throwables;
import io.airlift.slice.Slice;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;

import static com.fasterxml.jackson.core.JsonFactory.Feature.CANONICALIZE_FIELD_NAMES;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public abstract class Unnester
        implements Closeable
{
    protected static final JsonFactory JSON_FACTORY = new JsonFactory().disable(CANONICALIZE_FIELD_NAMES);
    protected static final int ESTIMATED_JSON_OUTPUT_SIZE = 512;

    private final int channelCount;
    private final JsonParser jsonParser;
    private boolean closed;

    protected Unnester(int channelCount, @Nullable Slice slice)
    {
        this.channelCount = channelCount;
        if (slice == null) {
            this.jsonParser = null;
            return;
        }

        try {
            this.jsonParser = JSON_FACTORY.createJsonParser(slice.getInput());
            JsonToken token = jsonParser.nextToken();
            checkState(token == JsonToken.START_ARRAY || token == JsonToken.START_OBJECT, "Expected start of array or object in input: \"%s\"", slice.toStringUtf8());
            readNextToken();
        }
        catch (IOException e) {
            close();
            throw Throwables.propagate(e);
        }
    }

    public final int getChannelCount()
    {
        return channelCount;
    }

    public final void appendNext(PageBuilder pageBuilder, int outputChannelOffset)
    {
        appendTo(pageBuilder, outputChannelOffset, jsonParser);
        readNextToken();
    }

    protected abstract void appendTo(PageBuilder pageBuilder, int outputChannelOffset, JsonParser jsonParser);

    public final boolean hasNext()
    {
        return jsonParser != null && !closed;
    }

    protected final void readNextToken()
    {
        JsonToken token;
        checkNotNull(jsonParser, "jsonParser is null");
        try {
            token = jsonParser.nextToken();
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
        if (token == null || token == JsonToken.END_ARRAY || token == JsonToken.END_OBJECT) {
            close();
        }
    }

    @Override
    public final void close()
    {
        if (closed) {
            return;
        }

        try {
            if (jsonParser != null) {
                jsonParser.close();
            }
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
        finally {
            closed = true;
        }
    }
}
