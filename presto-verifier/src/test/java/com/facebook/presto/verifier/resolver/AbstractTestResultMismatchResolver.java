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

import com.facebook.presto.verifier.checksum.ColumnMatchResult;
import com.facebook.presto.verifier.framework.QueryBundle;

import static com.facebook.presto.verifier.VerifierTestUtil.TEST_BUNDLE;
import static com.facebook.presto.verifier.resolver.FailureResolverTestUtil.createMatchResult;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public abstract class AbstractTestResultMismatchResolver
{
    private final FailureResolver failureResolver;

    public AbstractTestResultMismatchResolver(FailureResolver failureResolver)
    {
        this.failureResolver = requireNonNull(failureResolver, "failureResolver is null");
    }

    protected void assertResolved(ColumnMatchResult<?>... mismatchedColumns)
    {
        assertResolved(TEST_BUNDLE, mismatchedColumns);
    }

    protected void assertResolved(QueryBundle bundle, ColumnMatchResult<?>... mismatchedColumns)
    {
        assertTrue(failureResolver.resolveResultMismatch(createMatchResult(mismatchedColumns), bundle).isPresent());
    }

    protected void assertNotResolved(ColumnMatchResult<?>... mismatchedColumns)
    {
        assertNotResolved(TEST_BUNDLE, mismatchedColumns);
    }

    protected void assertNotResolved(QueryBundle bundle, ColumnMatchResult<?>... mismatchedColumns)
    {
        assertFalse(failureResolver.resolveResultMismatch(createMatchResult(mismatchedColumns), bundle).isPresent());
    }
}
