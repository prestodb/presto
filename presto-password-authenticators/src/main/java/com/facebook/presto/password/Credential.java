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
package com.facebook.presto.password;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public final class Credential
{
    private final String user;
    private final String password;

    public Credential(String username, String password)
    {
        this.user = requireNonNull(username, "username is null");
        this.password = requireNonNull(password, "password is null");
    }

    public String getUser()
    {
        return user;
    }

    public String getPassword()
    {
        return password;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        Credential o = (Credential) obj;
        return Objects.equals(user, o.getUser()) &&
                Objects.equals(password, o.getPassword());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(user, password);
    }
}
