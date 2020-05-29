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

import com.facebook.presto.spi.ErrorCodeSupplier;
import com.facebook.presto.verifier.framework.PrestoQueryException;
import com.facebook.presto.verifier.framework.QueryException;
import com.facebook.presto.verifier.framework.QueryStage;

import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

public class FailureResolverUtil
{
    private FailureResolverUtil() {}

    public static <T> Optional<T> mapMatchingPrestoException(
            QueryException queryException,
            QueryStage queryStage,
            Set<ErrorCodeSupplier> errorCodes,
            Function<PrestoQueryException, Optional<T>> mapper)
    {
        if (queryException.getQueryStage() != queryStage || !(queryException instanceof PrestoQueryException)) {
            return Optional.empty();
        }
        PrestoQueryException prestoException = (PrestoQueryException) queryException;
        if (!prestoException.getErrorCode().isPresent() || !errorCodes.contains(prestoException.getErrorCode().get())) {
            return Optional.empty();
        }
        return mapper.apply(prestoException);
    }
}
