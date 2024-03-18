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

import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.QueryRunner;

import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.createJavaIcebergQueryRunner;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.createNativeIcebergQueryRunner;

public class TestPrestoNativeIcebergPositionDeleteQueriesUsingThrift
        extends AbstractTestNativeIcebergPositionDeleteQueries
{
    private final String storageFormat = "PARQUET";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createNativeIcebergQueryRunner(true, storageFormat);
    }

    @Override
    protected ExpectedQueryRunner createExpectedQueryRunner()
            throws Exception
    {
        return createJavaIcebergQueryRunner(storageFormat);
    }
}
