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
package com.facebook.presto.pulsar;

import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import org.apache.pulsar.common.naming.TopicName;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.charset.Charset;
import java.util.Map;

import static com.facebook.presto.spi.type.BigintType.BIGINT;

@Test(singleThreaded = true)
public class TestPulsarRecordCursor extends TestPulsarConnector {

    private static final Logger log = Logger.get(TestPulsarRecordCursor.class);

    @Test
    public void testTopics() throws Exception {

        for (Map.Entry<TopicName, PulsarRecordCursor> entry : pulsarRecordCursors.entrySet()) {

            log.info("!------ topic %s ------!", entry.getKey());
            setup();
            PulsarRecordCursor pulsarRecordCursor = entry.getValue();
            TopicName topicName = entry.getKey();

            long count = 0L;
            while (pulsarRecordCursor.advanceNextPosition()) {
                for (int i = 0; i < fooColumnHandles.size(); i++) {

                    if (pulsarRecordCursor.isNull(i)) {

                    } else {
                        if (fooColumnHandles.get(i).getType().getJavaType() == long.class) {
                            if (fooColumnHandles.get(i).getType() == BIGINT) {
                                Assert.assertEquals(pulsarRecordCursor.getLong(i), count);
                            }
                        } else if (fooColumnHandles.get(i).getType().getJavaType() == boolean.class) {
                            Assert.assertEquals(pulsarRecordCursor.getBoolean(i), count % 2 == 0);
                        } else if (fooColumnHandles.get(i).getType().getJavaType() == double.class) {
                            Assert.assertEquals(pulsarRecordCursor.getDouble(i), Long.valueOf(count).doubleValue());
                        } else if (fooColumnHandles.get(i).getType().getJavaType() == Slice.class) {
                            if (!fooColumnHandles.get(i).isInternal()) {
                                Assert.assertEquals(pulsarRecordCursor.getSlice(i).toStringUtf8().getBytes(),
                                        Charset.forName("UTF-8").encode(String.valueOf(count)).array());
                            }
                        } else {
                            Assert.fail("Unknown type: " + fooColumnHandles.get(i).getType().getJavaType());
                        }
                    }
                }
                count++;
            }
            Assert.assertEquals(count, topicsToEntries.get(topicName.getSchemaName()).longValue());
            cleanup();
        }
    }
}
