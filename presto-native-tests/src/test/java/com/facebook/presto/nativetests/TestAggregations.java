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
package com.facebook.presto.nativetests;

import com.facebook.presto.testing.QueryRunner;
import org.testng.annotations.BeforeClass;

import static java.lang.Boolean.parseBoolean;

public class TestAggregations
        extends AbstractTestAggregationsNative
{
    private String storageFormat;
    private boolean charNToVarcharImplicitCast;
    private boolean sidecarEnabled;

    @BeforeClass
    @Override
    public void init()
            throws Exception
    {
        storageFormat = System.getProperty("storageFormat", "PARQUET");
        sidecarEnabled = parseBoolean(System.getProperty("sidecarEnabled", "false"));
        charNToVarcharImplicitCast = NativeTestsUtils.getCharNToVarcharImplicitCastForTest(
                sidecarEnabled, parseBoolean(System.getProperty("charNToVarcharImplicitCast", "false")));
        super.init(storageFormat, charNToVarcharImplicitCast, sidecarEnabled);
        super.init();
    }

    @Override
    protected QueryRunner createQueryRunner() throws Exception
    {
        return NativeTestsUtils.createNativeQueryRunner(storageFormat, charNToVarcharImplicitCast, sidecarEnabled);
    }

    @Override
    protected void createTables()
    {
        NativeTestsUtils.createTables(storageFormat);
    }
}
