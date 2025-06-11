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
package com.facebook.presto.rewriter.password;

import java.security.Principal;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public final class BasicPrincipal
        implements Principal
{
    private final String name;
    private final String role;

    public BasicPrincipal(String name, String role)
    {
        this.name = requireNonNull(name, "name is null");
        this.role = requireNonNull(role, "role is null");
    }

    public String getRole()
    {
        return role;
    }

    @Override
    public String getName()
    {
        return name;
    }

    @Override
    public String toString()
    {
        return String.format("Name: %s - Role: %s", name, role);
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
        BasicPrincipal that = (BasicPrincipal) o;
        return Objects.equals(name, that.getName()) && Objects.equals(role, that.getRole());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name);
    }
}
