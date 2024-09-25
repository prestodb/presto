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
package com.facebook.presto.nativeworker;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestPrestoNativeDynamicLibrary
        extends AbstractTestQueryFramework
{
    @Override
    protected ContainerQueryRunner createQueryRunner()
            throws Exception
    {
        return new ContainerQueryRunner();
    }
    @Test
    public void testShowFunctions()
            throws IOException, InterruptedException
    {
        TimeUnit.SECONDS.sleep(20);
        String sql = "SHOW FUNCTIONS";
        assertTrue(
                computeActual(sql).toString().contains("dynamic_123"), "dynamic function was not loaded");
    }
}
