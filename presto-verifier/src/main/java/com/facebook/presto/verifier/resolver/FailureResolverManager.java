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

import com.facebook.presto.verifier.framework.DataMatchResult;
import com.facebook.presto.verifier.framework.QueryBundle;
import com.facebook.presto.verifier.framework.QueryException;
import com.facebook.presto.verifier.framework.QueryObjectBundle;
import com.facebook.presto.verifier.prestoaction.QueryActionStats;

import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class FailureResolverManager
{
    private final Set<FailureResolver> failureResolvers;

    public FailureResolverManager(Set<FailureResolver> failureResolvers)
    {
        this.failureResolvers = requireNonNull(failureResolvers, "failureResolvers is null");
    }

    public Optional<String> resolveException(QueryActionStats controlStats, Throwable throwable, Optional<QueryObjectBundle> test)
    {
        if (!(throwable instanceof QueryException)) {
            return Optional.of("Verifier Error");
        }

        for (FailureResolver failureResolver : failureResolvers) {
            if (!controlStats.getQueryStats().isPresent()) {
                continue;
            }
            Optional<String> resolveMessage = failureResolver.resolveQueryFailure(controlStats.getQueryStats().get(), (QueryException) throwable, test);
            if (resolveMessage.isPresent()) {
                return resolveMessage;
            }
        }
        return Optional.empty();
    }

    public Optional<String> resolveResultMismatch(DataMatchResult matchResult, QueryBundle control)
    {
        for (FailureResolver failureResolver : failureResolvers) {
            Optional<String> resolveMessage = failureResolver.resolveResultMismatch(matchResult, control);
            if (resolveMessage.isPresent()) {
                return resolveMessage;
            }
        }
        return Optional.empty();
    }
}
