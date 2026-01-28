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
import com.google.common.collect.ImmutableMap;

public class TestPrestoNativeWindowQueries
        extends AbstractTestNativeWindowQueries
{
    @Override
    protected QueryRunner createQueryRunner() throws Exception
    {
        return PrestoNativeQueryRunnerUtils.nativeHiveQueryRunnerBuilder()
                .setAddStorageFormatToPath(true)
                .setUseThrift(true)
                .setExtraCoordinatorProperties(ImmutableMap.of("optimizer.optimize-top-n-rank", "true"))
                .build();
    }

    @Override
    protected ExpectedQueryRunner createExpectedQueryRunner() throws Exception
    {
        return PrestoNativeQueryRunnerUtils.javaHiveQueryRunnerBuilder()
                .setAddStorageFormatToPath(true)
                .build();
    }
}
