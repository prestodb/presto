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

package com.facebook.presto.eventlistener;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Map;

import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.recordDefaults;
public class TestEventListenerConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(EventListenerConfig.class)
                .setEventListenerFiles(""));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("event-listener.config-files", "a,b,c")
                .build();

        EventListenerConfig expected = new EventListenerConfig()
                .setEventListenerFiles("a,b,c");
        assertFullMapping(properties, expected);

        ImmutableList.Builder<File> filesBuilder = ImmutableList.builder();
        filesBuilder.add(new File("a"), new File("b"), new File("c"));
        //Test List version
        expected = new EventListenerConfig()
                .setEventListenerFiles(filesBuilder.build());

        assertFullMapping(properties, expected);
    }
}
