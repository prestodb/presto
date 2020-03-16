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

import static java.lang.System.identityHashCode;
import static java.util.Objects.requireNonNull;

public class VerificationRef
{
    private final Verification verification;

    public VerificationRef(Verification verification)
    {
        this.verification = requireNonNull(verification, "verification is null");
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        VerificationRef other = (VerificationRef) o;
        return verification == other.verification;
    }

    @Override
    public int hashCode()
    {
        return identityHashCode(verification);
    }
}
