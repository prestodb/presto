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
package com.facebook.presto.spi.security;

import com.facebook.presto.spi.PrestoException;

import java.util.Objects;

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Objects.requireNonNull;

public class Privilege
{
    public enum Type
    {
        SELECT,
        INSERT,
        DELETE
    }

    private final Type type;

    public Privilege(Type type)
    {
        this.type = requireNonNull(type, "type is null");
    }

    public Privilege(String typeString)
    {
        switch (typeString) {
            case "SELECT":
                type = Type.SELECT;
                break;
            case "INSERT":
                type = Type.INSERT;
                break;
            case "DELETE":
                type = Type.DELETE;
                break;
            default:
                throw new PrestoException(NOT_SUPPORTED, "Unsupported privilege: " + typeString);
        }
    }

    public Type getType()
    {
        return type;
    }

    public String getTypeString()
    {
        switch (this.type) {
            case SELECT:
                return "SELECT";
            case INSERT:
                return "INSERT";
            case DELETE:
                return "DELETE";
        }
        return null;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(type);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || (getClass() != obj.getClass())) {
            return false;
        }
        Privilege o = (Privilege) obj;
        return Objects.equals(type, o.getType());
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("Privilege{");
        sb.append("type=").append(type).append("}");
        return sb.toString();
    }
}
