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
package com.facebook.presto.verifier;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.base.Throwables;
import io.airlift.event.client.AbstractEventClient;
import io.airlift.event.client.JsonEventSerializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;

import static com.google.common.base.Preconditions.checkNotNull;

public class JsonEventClient
        extends AbstractEventClient
{
    // TODO we should use JsonEventWriter instead
    private final JsonEventSerializer serializer = new JsonEventSerializer(VerifierQueryEvent.class);
    private final JsonFactory factory = new JsonFactory();
    private final PrintStream out;

    public JsonEventClient(PrintStream out)
    {
        this.out = checkNotNull(out, "out is null");
    }

    @Override
    public <T> void postEvent(T event)
            throws IOException
    {
        try {
            ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            JsonGenerator generator = factory.createGenerator(buffer, JsonEncoding.UTF8);
            serializer.serialize(event, generator);
            out.println(buffer.toString().trim());
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }
}
