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
package com.facebook.presto.verifier.event;

import com.facebook.airlift.event.client.AbstractEventClient;
import com.facebook.airlift.event.client.JsonEventSerializer;
import com.facebook.presto.verifier.framework.VerifierConfig;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;

import javax.inject.Inject;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;

import static com.fasterxml.jackson.core.JsonEncoding.UTF8;
import static java.util.Objects.requireNonNull;

public class JsonEventClient
        extends AbstractEventClient
{
    private static final JsonEventSerializer serializer = new JsonEventSerializer(VerifierQueryEvent.class);
    private static final JsonFactory factory = new JsonFactory();

    private final PrintStream out;

    @Inject
    public JsonEventClient(VerifierConfig config)
            throws FileNotFoundException
    {
        requireNonNull(config.getJsonEventLogFile(), "jsonEventLogFile is null");
        this.out = config.getJsonEventLogFile().isPresent() ? new PrintStream(config.getJsonEventLogFile().get()) : System.out;
    }

    @Override
    public <T> void postEvent(T event)
            throws IOException
    {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        JsonGenerator generator = factory.createGenerator(buffer, UTF8);
        serializer.serialize(event, generator);
        out.println(buffer.toString().trim());
    }
}
