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
package com.facebook.presto.spark.execution;

import com.facebook.airlift.json.JsonCodec;
import org.testng.annotations.Test;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestBroadcastFileInfo
{
    private static final JsonCodec<BroadcastFileInfo> CODEC = jsonCodec(BroadcastFileInfo.class);

    @Test
    public void testRoundTripWithDescriptor()
    {
        BroadcastFileInfo info = CODEC.fromJson(CODEC.toJson(new BroadcastFileInfo("/tmp/file.bin", "c2VjcmV0LXRva2Vu")));

        assertEquals(info.getFilePath(), "/tmp/file.bin");
        assertEquals(info.getDescriptor(), "c2VjcmV0LXRva2Vu");
    }

    @Test
    public void testRoundTripWithoutDescriptor()
    {
        BroadcastFileInfo info = CODEC.fromJson(CODEC.toJson(new BroadcastFileInfo("/tmp/file.bin", null)));

        assertEquals(info.getFilePath(), "/tmp/file.bin");
        assertNull(info.getDescriptor());
    }

    /**
     * A writer that predates the descriptor emits the field not at all. The reader must still
     * accept the payload and fall back to opening by path.
     */
    @Test
    public void testAbsentDescriptorFieldDeserializes()
    {
        BroadcastFileInfo info = CODEC.fromJson("{\"filePath\": \"/tmp/file.bin\"}");

        assertEquals(info.getFilePath(), "/tmp/file.bin");
        assertNull(info.getDescriptor());
    }

    /**
     * The descriptor carries Warm Storage bearer tokens, so toString() must not expose it.
     */
    @Test
    public void testToStringOmitsTheDescriptor()
    {
        String rendered = new BroadcastFileInfo("/tmp/file.bin", "c2VjcmV0LXRva2Vu").toString();

        assertFalse(rendered.contains("c2VjcmV0LXRva2Vu"), rendered);
        assertTrue(rendered.contains("BroadcastFileInfo"), rendered);
    }
}
