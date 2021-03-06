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
package com.facebook.presto.sql.planner.iterative.rule.test;

import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.Plugin;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import java.util.List;

import static com.facebook.airlift.testing.Closeables.closeAllRuntimeException;

public abstract class BaseRuleTest
{
    private RuleTester tester;
    private List<Plugin> plugins;

    public BaseRuleTest(Plugin... plugins)
    {
        this.plugins = ImmutableList.copyOf(plugins);
    }

    @BeforeClass
    public final void setUp()
    {
        tester = new RuleTester(plugins);
    }

    @AfterClass(alwaysRun = true)
    public final void tearDown()
    {
        closeAllRuntimeException(tester);
        tester = null;
    }

    protected RuleTester tester()
    {
        return tester;
    }

    protected Metadata getMetadata()
    {
        return tester.getMetadata();
    }

    protected FunctionAndTypeManager getFunctionManager()
    {
        return tester.getMetadata().getFunctionAndTypeManager();
    }
}
