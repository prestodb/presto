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
package com.facebook.presto.nativetests.cudf;

import com.facebook.presto.nativeworker.AbstractTestNativeTpchQueries;
import com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils;
import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.QueryRunner;

public class TestCudfTpchQueries
        extends AbstractTestNativeTpchQueries
{
    private static final String DEFAULT_STORAGE_FORMAT = "PARQUET";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        String storageFormat = System.getProperty("storageFormat", DEFAULT_STORAGE_FORMAT);
        return PrestoNativeQueryRunnerUtils.nativeHiveQueryRunnerBuilder()
                .setStorageFormat(storageFormat)
                .setAddStorageFormatToPath(true)
                .setCoordinatorSidecarEnabled(false)
                .setEnableCudf(true)
                .build();
    }

    @Override
    protected ExpectedQueryRunner createExpectedQueryRunner()
            throws Exception
    {
        String storageFormat = System.getProperty("storageFormat", DEFAULT_STORAGE_FORMAT);
        return PrestoNativeQueryRunnerUtils.javaHiveQueryRunnerBuilder()
                .setStorageFormat(storageFormat)
                .setAddStorageFormatToPath(true)
                .build();
    }
}
