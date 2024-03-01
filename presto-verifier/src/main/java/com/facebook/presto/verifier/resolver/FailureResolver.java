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
import com.facebook.presto.verifier.framework.DataMatchResult;
import com.facebook.presto.verifier.framework.QueryBundle;
import com.facebook.presto.verifier.framework.QueryException;
import com.facebook.presto.verifier.framework.QueryObjectBundle;

import java.util.Optional;

public interface FailureResolver
{
    default Optional<String> resolveQueryFailure(QueryStats controlQueryStats, QueryException queryException, Optional<QueryObjectBundle> test)
    {
        return Optional.empty();
    }

    default Optional<String> resolveResultMismatch(DataMatchResult matchResult, QueryBundle control)
    {
        return Optional.empty();
    }
}
