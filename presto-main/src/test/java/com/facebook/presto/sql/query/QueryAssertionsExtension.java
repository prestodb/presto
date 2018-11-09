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
package com.facebook.presto.sql.query;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;

import java.util.function.Supplier;

import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

public class QueryAssertionsExtension
        implements ParameterResolver, BeforeAllCallback, AfterAllCallback
{
    private final Supplier<QueryAssertions> queryAssertionsSupplier;
    private QueryAssertions queryAssertions;

    public QueryAssertionsExtension()
    {
        this(() -> new QueryAssertions());
    }

    public QueryAssertionsExtension(Supplier<QueryAssertions> queryAssertionsSupplier)
    {
        this.queryAssertionsSupplier = requireNonNull(queryAssertionsSupplier, "queryAssertionsSupplier is null");
    }

    @Override
    public void beforeAll(ExtensionContext extensionContext)
    {
        verify(queryAssertions == null, "QueryAssertions already created");
        queryAssertions = queryAssertionsSupplier.get();
    }

    @Override
    public void afterAll(ExtensionContext extensionContext)
    {
        verify(queryAssertions != null, "QueryAssertions is not created");
        queryAssertions.close();
        queryAssertions = null;
    }

    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
            throws ParameterResolutionException
    {
        return parameterContext.getParameter().getType().equals(QueryAssertions.class);
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
            throws ParameterResolutionException
    {
        return queryAssertions;
    }
}
