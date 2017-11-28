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
package com.facebook.presto.spi.security;

import io.airlift.json.JsonCodec;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.airlift.json.JsonCodec.jsonCodec;
import static org.testng.Assert.assertEquals;

public class TestSelectedRole
{
    private static final JsonCodec<SelectedRole> SELECTED_ROLE_JSON_CODEC = jsonCodec(SelectedRole.class);

    @Test
    public void testJsonSerialization()
            throws Exception
    {
        assertJsonRoundTrip(new SelectedRole(SelectedRole.Type.ALL, Optional.empty()));
        assertJsonRoundTrip(new SelectedRole(SelectedRole.Type.NONE, Optional.empty()));
        assertJsonRoundTrip(new SelectedRole(SelectedRole.Type.ROLE, Optional.of("role")));
    }

    private static void assertJsonRoundTrip(SelectedRole expected)
    {
        assertEquals(SELECTED_ROLE_JSON_CODEC.fromJson(SELECTED_ROLE_JSON_CODEC.toJson(expected)), expected);
    }

    @Test
    public void testToStringSerialization()
            throws Exception
    {
        assertToStringRoundTrip(new SelectedRole(SelectedRole.Type.ALL, Optional.empty()));
        assertToStringRoundTrip(new SelectedRole(SelectedRole.Type.NONE, Optional.empty()));
        assertToStringRoundTrip(new SelectedRole(SelectedRole.Type.ROLE, Optional.of("role")));
    }

    private static void assertToStringRoundTrip(SelectedRole expected)
    {
        assertEquals(SelectedRole.valueOf(expected.toString()), expected);
    }
}
