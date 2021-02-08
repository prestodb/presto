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
package com.facebook.presto.common.type;

import com.facebook.presto.common.QualifiedObjectName;

import java.util.Objects;
import java.util.Optional;

import static java.lang.String.format;
import static java.util.Locale.ENGLISH;

public class TypeSignatureBase
{
    private final Optional<String> standardTypeBase;
    private final Optional<QualifiedObjectName> typeBase;

    public static TypeSignatureBase of(String name)
    {
        if (name.chars().noneMatch(c -> c == '.')) {
            return new TypeSignatureBase(name);
        }
        return new TypeSignatureBase(QualifiedObjectName.valueOf(name.toLowerCase(ENGLISH)));
    }

    public static TypeSignatureBase of(QualifiedObjectName name)
    {
        return new TypeSignatureBase(name);
    }

    private TypeSignatureBase(String standardTypeBase)
    {
        checkArgument(standardTypeBase != null, "standardTypeBase is null");
        checkArgument(!standardTypeBase.isEmpty(), "standardTypeBase is empty");
        checkArgument(standardTypeBase.chars().noneMatch(c -> c == '.'), "Standard type %s should not have '.' in it", standardTypeBase);
        checkArgument(validateName(standardTypeBase), "Bad characters in base type: %s", standardTypeBase);
        this.standardTypeBase = Optional.of(standardTypeBase);
        this.typeBase = Optional.empty();
    }

    private TypeSignatureBase(QualifiedObjectName typeBase)
    {
        checkArgument(typeBase != null, "typeBase is null");
        checkArgument(validateName(typeBase.getObjectName()), "Bad characters in base type: %s", typeBase);
        this.standardTypeBase = Optional.empty();
        this.typeBase = Optional.of(typeBase);
    }

    public boolean isStandardType()
    {
        return standardTypeBase.isPresent();
    }

    public QualifiedObjectName getQualifiedObjectName()
    {
        checkArgument(typeBase.isPresent(), "TypeSignatureBase %s is not a QualifiedObjectName", toString());
        return typeBase.get();
    }

    private static boolean validateName(String name)
    {
        return name.chars().noneMatch(c -> c == '<' || c == '>' || c == ',');
    }

    private static void checkArgument(boolean argument, String format, Object... args)
    {
        if (!argument) {
            throw new IllegalArgumentException(format(format, args));
        }
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        TypeSignatureBase other = (TypeSignatureBase) obj;

        return Objects.equals(standardTypeBase.map(s -> s.toLowerCase(ENGLISH)), other.standardTypeBase.map(s -> s.toLowerCase(ENGLISH))) &&
                Objects.equals(typeBase, other.typeBase);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(standardTypeBase.map(s -> s.toLowerCase(ENGLISH)), typeBase);
    }

    @Override
    public String toString()
    {
        return standardTypeBase.orElseGet(() -> typeBase.get().toString());
    }
}
