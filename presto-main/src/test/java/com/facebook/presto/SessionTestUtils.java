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
package com.facebook.presto;

import com.facebook.presto.client.ClientCapabilities;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Arrays.stream;

public final class SessionTestUtils
{
    public static final Session TEST_SESSION = testSessionBuilder()
            .setCatalog("tpch")
            .setSchema(TINY_SCHEMA_NAME)
            .setClientCapabilities(stream(ClientCapabilities.values())
                    .map(ClientCapabilities::toString)
                    .collect(toImmutableSet()))
            .build();

    private SessionTestUtils()
    {
    }
}
