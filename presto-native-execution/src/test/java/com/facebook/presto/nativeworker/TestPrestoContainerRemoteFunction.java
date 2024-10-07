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

import static org.testng.Assert.assertEquals;

public class TestPrestoContainerRemoteFunction
        extends AbstractTestQueryFramework
{
    private ContainerQueryRunner queryRunner;

    @Override
    protected ContainerQueryRunner createQueryRunner()
            throws Exception
    {
        queryRunner = new ContainerQueryRunner();
        return queryRunner;
    }

    @Test
    public void testPresenceAndBasicFunctionality()
    {
        assertEquals(computeActual("select remote.default.abs(-10)").getMaterializedRows().get(0).getField(0).toString(), "10");
    }
}
