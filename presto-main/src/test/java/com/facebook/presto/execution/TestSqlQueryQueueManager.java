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
package com.facebook.presto.execution;

import io.airlift.json.ObjectMapperProvider;
import org.testng.annotations.Test;
import org.weakref.jmx.MBeanExporter;

import java.lang.management.ManagementFactory;
import java.util.regex.Pattern;

import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestSqlQueryQueueManager
{
    @Test
    public void testJsonParsing()
    {
        parse("queue_config.json");
        assertFails("queue_config_bad_cycle.json", "Queues must not contain a cycle. The shortest cycle found is \\[q(.), q., q., q., q\\1\\]");
        assertFails("queue_config_bad_selfcycle.json", "Queues must not contain a cycle. The shortest cycle found is \\[q1, q1\\]");
        assertFails("queue_config_bad_out_degree.json", "Queues must form a tree. Queue q0 feeds into \\[q1, q2\\]");
    }

    private void parse(String fileName)
    {
        String path = this.getClass().getClassLoader().getResource(fileName).getPath();
        QueryManagerConfig config = new QueryManagerConfig();
        config.setQueueConfigFile(path);
        new SqlQueryQueueManager(new QueryQueueRuleFactory(config, new ObjectMapperProvider().get()).get(), new MBeanExporter(ManagementFactory.getPlatformMBeanServer()));
    }

    private void assertFails(String fileName, String expectedPattern)
    {
        try {
            parse(fileName);
            fail("Expected to throw an IllegalArgumentException with message " + expectedPattern);
        }
        catch (IllegalArgumentException e) {
            assertTrue(Pattern.matches(expectedPattern, e.getMessage()),
                    "\nExpected (re) :" + expectedPattern + "\nActual        :" + e.getMessage());
        }
    }
}
