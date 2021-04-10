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
package com.facebook.presto.orc;

import com.facebook.presto.orc.StreamLayout.ByColumnSize;
import com.facebook.presto.orc.StreamLayout.ByStreamSize;
import com.facebook.presto.orc.metadata.Stream;
import com.facebook.presto.orc.metadata.Stream.StreamKind;
import com.facebook.presto.orc.stream.StreamDataOutput;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class TestStreamLayout
{
    private static StreamDataOutput createStream(int column, StreamKind streamKind, int length)
    {
        Stream stream = new Stream(column, streamKind, length, true);
        return new StreamDataOutput(Slices.allocate(1024), stream);
    }

    private static void verifyStream(Stream stream, int column, StreamKind streamKind, int length)
    {
        assertEquals(stream.getColumn(), column);
        assertEquals(stream.getLength(), length);
        assertEquals(stream.getStreamKind(), streamKind);
    }

    @Test
    public void testByStreamSize()
    {
        List<StreamDataOutput> streams = new ArrayList<>();
        int length = 10_000;
        for (int i = 0; i < 10; i++) {
            streams.add(createStream(i, StreamKind.PRESENT, length - i));
            streams.add(createStream(i, StreamKind.DATA, length - 100 - i));
        }

        Collections.shuffle(streams);

        new ByStreamSize().reorder(streams);

        assertEquals(streams.size(), 20);
        Iterator<StreamDataOutput> iterator = streams.iterator();
        for (int i = 9; i >= 0; i--) {
            verifyStream(iterator.next().getStream(), i, StreamKind.DATA, length - 100 - i);
        }

        for (int i = 9; i >= 0; i--) {
            verifyStream(iterator.next().getStream(), i, StreamKind.PRESENT, length - i);
        }
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testByColumnSize()
    {
        // Assume the file has 3 streams
        // 1st Column( 1010), Data(1000), Present(10)
        // 2nd column (1010), Dictionary (300), Present (10), Data(600), Length(100)
        // 3rd Column > 2GB

        List<StreamDataOutput> streams = new ArrayList<>();
        streams.add(createStream(1, StreamKind.DATA, 1_000));
        streams.add(createStream(1, StreamKind.PRESENT, 10));

        streams.add(createStream(2, StreamKind.DICTIONARY_DATA, 300));
        streams.add(createStream(2, StreamKind.PRESENT, 10));
        streams.add(createStream(2, StreamKind.DATA, 600));
        streams.add(createStream(2, StreamKind.LENGTH, 100));

        streams.add(createStream(3, StreamKind.DATA, Integer.MAX_VALUE));
        streams.add(createStream(3, StreamKind.PRESENT, Integer.MAX_VALUE));

        Collections.shuffle(streams);
        new ByColumnSize().reorder(streams);

        Iterator<StreamDataOutput> iterator = streams.iterator();
        verifyStream(iterator.next().getStream(), 1, StreamKind.PRESENT, 10);
        verifyStream(iterator.next().getStream(), 1, StreamKind.DATA, 1000);

        verifyStream(iterator.next().getStream(), 2, StreamKind.PRESENT, 10);
        verifyStream(iterator.next().getStream(), 2, StreamKind.LENGTH, 100);
        verifyStream(iterator.next().getStream(), 2, StreamKind.DICTIONARY_DATA, 300);
        verifyStream(iterator.next().getStream(), 2, StreamKind.DATA, 600);

        verifyStream(iterator.next().getStream(), 3, StreamKind.PRESENT, Integer.MAX_VALUE);
        verifyStream(iterator.next().getStream(), 3, StreamKind.DATA, Integer.MAX_VALUE);

        assertFalse(iterator.hasNext());
    }
}
