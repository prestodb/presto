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
import com.facebook.presto.verifier.framework.QueryException;
import com.facebook.presto.verifier.framework.QueryObjectBundle;
import com.google.common.collect.ImmutableSet;

import java.util.Optional;

import static com.facebook.presto.spi.StandardErrorCode.EXCEEDED_TIME_LIMIT;
import static com.facebook.presto.verifier.framework.QueryStage.TEST_MAIN;
import static com.facebook.presto.verifier.resolver.FailureResolverUtil.mapMatchingPrestoException;

public class ExceededTimeLimitFailureResolver
        implements FailureResolver
{
    public static final String NAME = "exceeded-time-limit";

    @Override
    public Optional<String> resolveQueryFailure(QueryStats controlQueryStats, QueryException queryException, Optional<QueryObjectBundle> test)
    {
        return mapMatchingPrestoException(queryException, TEST_MAIN, ImmutableSet.of(EXCEEDED_TIME_LIMIT),
                e -> Optional.of("Time limit exceeded on test cluster"));
    }
}
