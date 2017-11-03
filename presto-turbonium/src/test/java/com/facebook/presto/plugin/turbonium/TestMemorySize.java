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
package com.facebook.presto.plugin.turbonium;

import io.airlift.log.Logger;
import org.openjdk.jol.info.ClassLayout;
import org.testng.annotations.Test;

import java.util.BitSet;

public class TestMemorySize
{
    private static final Logger log = Logger.get(TestMemorySize.class);

    public abstract static class AbstractTestClass
    {
        protected final int afoo = 16;
        protected final int abar = 16;
        //protected final int abaz = 16;
        //protected final int abat = 16;
    }
    public static class TestClass
            extends AbstractTestClass
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(TestClass.class).instanceSize();
        //private final int foo = 16;
        //private final int bar = 16;
        //private final Object o = new Object();
        private final BitSet bitSet = new BitSet(65);
        //private final long[] boo = {3L, 4L};
        public long size()
        {
            return INSTANCE_SIZE + (bitSet.size() >> 3);
        }
    }
    @Test
    public void testMemorySize()
    {
        TestClass t = new TestClass();
        log.info("Size is %s", t.size());
    }
}
