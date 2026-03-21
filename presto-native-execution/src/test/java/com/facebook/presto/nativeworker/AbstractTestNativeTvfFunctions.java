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

import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tvf.TvfPlugin;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createRegion;

@Test
public abstract class AbstractTestNativeTvfFunctions
        extends AbstractTestQueryFramework
{
    @BeforeClass
    @Override
    public void init() throws Exception
    {
        super.init();
        getQueryRunner().installCoordinatorPlugin(new TvfPlugin());
        getQueryRunner().loadTVFProvider("system");
    }

    @Override
    protected void createTables()
    {
        createRegion((QueryRunner) getExpectedQueryRunner());
    }

    @Test
    public void testSequence()
    {
        assertQuery("SELECT * FROM TABLE(sequence( start => 20, stop => 100, step => 5))");
    }

    @Test
    public void testExcludeColumns()
    {
        assertQuery("SELECT * FROM TABLE(exclude_columns(input => TABLE(region), columns => DESCRIPTOR(regionkey, comment)))");
    }
}
