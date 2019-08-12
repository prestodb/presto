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
package com.facebook.presto.verifier.resolver;

import com.facebook.presto.jdbc.QueryStats;
import com.facebook.presto.spi.ErrorCodeSupplier;

import java.util.Optional;

import static com.facebook.presto.spi.StandardErrorCode.EXCEEDED_TIME_LIMIT;
import static com.facebook.presto.verifier.framework.QueryStage.TEST_MAIN;

public class ExceededTimeLimitFailureResolver
        extends AbstractPrestoQueryFailureResolver
{
    public ExceededTimeLimitFailureResolver()
    {
        super(TEST_MAIN);
    }

    @Override
    public Optional<String> resolveTestQueryFailure(ErrorCodeSupplier errorCode, QueryStats controlQueryStats, QueryStats testQueryStats)
    {
        if (errorCode == EXCEEDED_TIME_LIMIT) {
            return Optional.of("Auto Resolved: Test cluster has less computing resource");
        }
        return Optional.empty();
    }
}
