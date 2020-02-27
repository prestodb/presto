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
import com.facebook.presto.verifier.framework.PrestoQueryException;
import com.facebook.presto.verifier.framework.QueryBundle;
import com.facebook.presto.verifier.framework.QueryException;
import com.facebook.presto.verifier.framework.QueryStage;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public abstract class AbstractPrestoQueryFailureResolver
        implements FailureResolver
{
    private QueryStage expectedQueryStage;

    public AbstractPrestoQueryFailureResolver(QueryStage expectedQueryStage)
    {
        this.expectedQueryStage = requireNonNull(expectedQueryStage, "expectedQueryOrigin is null");
    }

    public abstract Optional<String> resolveTestQueryFailure(ErrorCodeSupplier errorCode, QueryStats controlQueryStats, QueryStats testQueryStats, Optional<QueryBundle> test);

    @Override
    public Optional<String> resolve(QueryStats controlQueryStats, QueryException queryException, Optional<QueryBundle> test)
    {
        if (!(queryException instanceof PrestoQueryException)) {
            return Optional.empty();
        }
        PrestoQueryException prestoException = (PrestoQueryException) queryException;
        if (prestoException.getQueryStage() != expectedQueryStage ||
                !prestoException.getErrorCode().isPresent() ||
                !prestoException.getQueryStats().isPresent()) {
            return Optional.empty();
        }

        return resolveTestQueryFailure(prestoException.getErrorCode().get(), controlQueryStats, prestoException.getQueryStats().get(), test);
    }
}
