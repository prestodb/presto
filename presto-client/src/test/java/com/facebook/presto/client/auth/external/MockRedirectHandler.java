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
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

public class MockRedirectHandler
        implements RedirectHandler
{
    private URI redirectedTo;
    private AtomicInteger redirectionCount = new AtomicInteger(0);
    private Duration redirectTime;

    @Override
    public void redirectTo(URI uri)
            throws RedirectException
    {
        redirectedTo = uri;
        redirectionCount.incrementAndGet();
        try {
            if (redirectTime != null) {
                Thread.sleep(redirectTime.toMillis());
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    public URI redirectedTo()
    {
        return redirectedTo;
    }

    public int getRedirectionCount()
    {
        return redirectionCount.get();
    }

    public MockRedirectHandler sleepOnRedirect(Duration redirectTime)
    {
        this.redirectTime = redirectTime;
        return this;
    }
}
