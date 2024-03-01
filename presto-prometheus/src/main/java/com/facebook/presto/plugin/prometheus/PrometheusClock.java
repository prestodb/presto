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
package com.facebook.presto.plugin.prometheus;

import javax.inject.Inject;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

import static java.time.ZoneOffset.UTC;

// settable Clock for testing

public class PrometheusClock
{
    private final Clock clock;

    @Inject
    public PrometheusClock()
    {
        this(Clock.systemUTC());
    }

    private PrometheusClock(Clock clock)
    {
        this.clock = clock;
    }

    public static PrometheusClock fixedClockAt(LocalDateTime date)
    {
        return new PrometheusClock(Clock.fixed(date.atZone(ZoneId.systemDefault()).toInstant(), ZoneId.systemDefault()));
    }

    public Instant now()
    {
        return clock.instant();
    }

    public static PrometheusClock fixedClockAt(Instant fixedInstant)
    {
        return new PrometheusClock(Clock.fixed(fixedInstant, UTC));
    }
}
