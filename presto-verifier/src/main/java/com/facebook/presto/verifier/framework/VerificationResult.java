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
package com.facebook.presto.verifier.framework;

import com.facebook.presto.verifier.event.VerifierQueryEvent;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class VerificationResult
{
    private final Verification verification;
    private final boolean shouldResubmit;
    private final Optional<VerifierQueryEvent> event;

    public VerificationResult(Verification verification, boolean shouldResubmit, Optional<VerifierQueryEvent> event)
    {
        this.verification = requireNonNull(verification, "verification is null");
        this.shouldResubmit = requireNonNull(shouldResubmit, "shouldResubmit is null");
        this.event = requireNonNull(event, "event is null");
    }

    public Verification getVerification()
    {
        return verification;
    }

    public boolean shouldResubmit()
    {
        return shouldResubmit;
    }

    public Optional<VerifierQueryEvent> getEvent()
    {
        return event;
    }
}
