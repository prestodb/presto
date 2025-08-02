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
package com.facebook.presto.client.auth.external;

import java.net.URI;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class CompositeRedirectHandler
        implements RedirectHandler
{
    private final List<RedirectHandler> handlers;

    public CompositeRedirectHandler(List<ExternalRedirectStrategy> strategies)
    {
        this.handlers = requireNonNull(strategies, "strategies is null")
                .stream()
                .map(ExternalRedirectStrategy::getHandler)
                .collect(toImmutableList());
        checkState(!handlers.isEmpty(), "Expected at least one external redirect handler");
    }

    @Override
    public void redirectTo(URI uri) throws RedirectException
    {
        RedirectException redirectException = new RedirectException("Could not redirect to " + uri);
        for (RedirectHandler handler : handlers) {
            try {
                handler.redirectTo(uri);
                return;
            }
            catch (RedirectException e) {
                redirectException.addSuppressed(e);
            }
        }

        throw redirectException;
    }
}
