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
    private final Optional<QualifiedObjectName> typeName;

    public static TypeSignatureBase of(String name)
    {
        int pos = name.indexOf(":");
        if (pos >= 0) {
            return new TypeSignatureBase(QualifiedObjectName.valueOf(name.substring(0, pos)), name.substring(pos + 1));
        }
        if (name.chars().noneMatch(c -> c == '.')) {
            return new TypeSignatureBase(name);
        }
        return new TypeSignatureBase(QualifiedObjectName.valueOf(name.toLowerCase(ENGLISH)));
    }

    public static TypeSignatureBase of(QualifiedObjectName name)
    {
        return new TypeSignatureBase(name);
    }

    public static TypeSignatureBase of(UserDefinedType userDefinedType)
    {
        return new TypeSignatureBase(userDefinedType.getUserDefinedTypeName(), userDefinedType.getPhysicalTypeSignature().getTypeSignatureBase().getStandardTypeBase());
    }

    private TypeSignatureBase(String standardTypeBase)
    {
        checkArgument(standardTypeBase != null, "standardTypeBase is null");
        checkArgument(!standardTypeBase.isEmpty(), "standardTypeBase is empty");
        checkArgument(standardTypeBase.chars().noneMatch(c -> c == '.'), "Standard type %s should not have '.' in it", standardTypeBase);
        checkArgument(validateName(standardTypeBase), "Bad characters in base type: %s", standardTypeBase);
        this.standardTypeBase = Optional.of(standardTypeBase);
        this.typeName = Optional.empty();
    }

    private TypeSignatureBase(QualifiedObjectName typeName)
    {
        checkArgument(typeName != null, "typeName is null");
        checkArgument(validateName(typeName.getObjectName()), "Bad characters in typeName: %s", typeName);
        this.standardTypeBase = Optional.empty();
        this.typeName = Optional.of(typeName);
    }

    private TypeSignatureBase(QualifiedObjectName typeName, String standardTypeBase)
    {
        checkArgument(typeName != null && standardTypeBase != null, "typeName or standardTypeBase is null");
        checkArgument(validateName(typeName.getObjectName()), "Bad characters in type name: %s", typeName.getObjectName());
        checkArgument(validateName(standardTypeBase), "Bad characters in base type: %s", standardTypeBase);
        this.standardTypeBase = Optional.of(standardTypeBase);
        this.typeName = Optional.of(typeName);
    }

    public boolean hasStandardType()
    {
        return standardTypeBase.isPresent();
    }

    public boolean hasTypeName()
    {
        return typeName.isPresent();
    }

    public QualifiedObjectName getTypeName()
    {
        checkArgument(typeName.isPresent(), "TypeSignatureBase %s does not have type name", toString());
        return typeName.get();
    }

    public String getStandardTypeBase()
    {
        checkArgument(standardTypeBase.isPresent(), "TypeSignatureBase %s does not have standard type base", toString());
        return standardTypeBase.get();
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
                Objects.equals(typeName, other.typeName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(standardTypeBase.map(s -> s.toLowerCase(ENGLISH)), typeName);
    }

    @Override
    public String toString()
    {
        if (standardTypeBase.isPresent() && typeName.isPresent()) {
            // user defined type
            return format("%s:%s", typeName.get(), standardTypeBase.get());
        }
        return standardTypeBase.orElseGet(() -> typeName.get().toString());
    }
}
