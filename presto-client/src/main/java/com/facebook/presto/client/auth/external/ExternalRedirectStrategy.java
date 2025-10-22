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

import com.google.common.collect.ImmutableList;

import static java.util.Objects.requireNonNull;

public enum ExternalRedirectStrategy
{
    DESKTOP_OPEN(new DesktopBrowserRedirectHandler()),
    SYSTEM_OPEN(new SystemOpenRedirectHandler()),
    PRINT(new SystemOutPrintRedirectHandler()),
    OPEN(new CompositeRedirectHandler(ImmutableList.of(SYSTEM_OPEN, DESKTOP_OPEN))),
    ALL(new CompositeRedirectHandler(ImmutableList.of(OPEN, PRINT)))
    /**/;

    private final RedirectHandler handler;

    ExternalRedirectStrategy(RedirectHandler handler)
    {
        this.handler = requireNonNull(handler, "handler is null");
    }

    public RedirectHandler getHandler()
    {
        return handler;
    }
}
