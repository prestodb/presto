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
package com.facebook.presto.sql.query;

import com.facebook.presto.Session;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;

public abstract class BaseQueryAssertionsTest
{
    private QueryAssertions assertions;

    @BeforeClass
    public void init()
    {
        assertions = new QueryAssertions(getDefaultSession());
    }

    protected Session getDefaultSession()
    {
        return testSessionBuilder()
                .setCatalog("local")
                .setSchema("default")
                .build();
    }

    @AfterClass(alwaysRun = true)
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    protected QueryAssertions assertions()
    {
        return assertions;
    }
}
