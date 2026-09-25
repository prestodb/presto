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
package com.facebook.presto.testng.services;

import com.fasterxml.jackson.core.StreamReadConstraints;
import org.testng.IExecutionListener;

/**
 * TestNG execution listener that lifts Jackson 2.18+'s default 50,000-character
 * JSON property-name limit before any test class is loaded.
 *
 * Jackson 2.18 introduced {@link StreamReadConstraints} with a default
 * {@code maxNameLength} of 50,000. The static memoized {@code ObjectMapper}
 * inside {@code JsonCodec} (airlift) is built lazily by {@code JsonObjectMapperProvider},
 * which calls {@code new JsonFactory()} and bakes in the defaults at that moment.
 * Calling {@link StreamReadConstraints#overrideDefaultStreamReadConstraints} here —
 * in {@code onExecutionStart}, which fires before any test class is loaded —
 * ensures every subsequent {@code JsonFactory} construction (including the one
 * inside {@code JsonCodec}) uses an unlimited name length, mirroring what
 * {@code PrestoServer.run()} does for the production server.
 */
public class JacksonStreamConstraintsListener
        implements IExecutionListener
{
    @Override
    public void onExecutionStart()
    {
        StreamReadConstraints.overrideDefaultStreamReadConstraints(
                StreamReadConstraints.builder()
                        .maxNameLength(Integer.MAX_VALUE)
                        .build());
    }
}
