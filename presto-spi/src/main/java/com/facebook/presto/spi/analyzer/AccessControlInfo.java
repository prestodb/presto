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
package com.facebook.presto.spi.analyzer;

import com.facebook.presto.spi.security.AccessControl;
import com.facebook.presto.spi.security.Identity;

import java.util.Objects;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class AccessControlInfo
{
    private final AccessControl accessControl;
    private final Identity identity;

    public AccessControlInfo(AccessControl accessControl, Identity identity)
    {
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.identity = requireNonNull(identity, "identity is null");
    }

    public AccessControl getAccessControl()
    {
        return accessControl;
    }

    public Identity getIdentity()
    {
        return identity;
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

        AccessControlInfo that = (AccessControlInfo) o;
        return Objects.equals(accessControl, that.accessControl) &&
                Objects.equals(identity, that.identity);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(accessControl, identity);
    }

    @Override
    public String toString()
    {
        return format("AccessControl: %s, Identity: %s", accessControl.getClass(), identity);
    }
}
