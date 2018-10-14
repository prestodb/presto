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
package com.facebook.presto.dispatcher;

import java.net.URI;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class CoordinatorLocation
{
    private final Optional<URI> httpUri;
    private final Optional<URI> httpsUri;

    public CoordinatorLocation(Optional<URI> httpUri, Optional<URI> httpsUri)
    {
        this.httpUri = requireNonNull(httpUri, "httpUri is null");
        this.httpsUri = requireNonNull(httpsUri, "httpsUri is null");
        checkArgument(httpUri.isPresent() || httpsUri.isPresent(), "Coordinator must have a HTTP or HTTPS port");
    }

    public URI getUri(String preferredScheme)
    {
        if ("https".equalsIgnoreCase(preferredScheme)) {
            return httpsUri.orElseGet(httpUri::get);
        }
        else {
            return httpUri.orElseGet(httpsUri::get);
        }
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .omitNullValues()
                .add("httpUri", httpUri.orElse(null))
                .add("httpsUri", httpsUri.orElse(null))
                .toString();
    }
}
