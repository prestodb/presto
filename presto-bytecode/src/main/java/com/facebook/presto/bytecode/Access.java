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
package com.facebook.presto.bytecode;

import com.google.common.collect.ImmutableList;

import java.util.EnumSet;

import static java.util.Locale.ENGLISH;
import static org.objectweb.asm.Opcodes.ACC_ABSTRACT;
import static org.objectweb.asm.Opcodes.ACC_ANNOTATION;
import static org.objectweb.asm.Opcodes.ACC_BRIDGE;
import static org.objectweb.asm.Opcodes.ACC_ENUM;
import static org.objectweb.asm.Opcodes.ACC_FINAL;
import static org.objectweb.asm.Opcodes.ACC_INTERFACE;
import static org.objectweb.asm.Opcodes.ACC_NATIVE;
import static org.objectweb.asm.Opcodes.ACC_PRIVATE;
import static org.objectweb.asm.Opcodes.ACC_PROTECTED;
import static org.objectweb.asm.Opcodes.ACC_PUBLIC;
import static org.objectweb.asm.Opcodes.ACC_STATIC;
import static org.objectweb.asm.Opcodes.ACC_STRICT;
import static org.objectweb.asm.Opcodes.ACC_SUPER;
import static org.objectweb.asm.Opcodes.ACC_SYNCHRONIZED;
import static org.objectweb.asm.Opcodes.ACC_SYNTHETIC;
import static org.objectweb.asm.Opcodes.ACC_TRANSIENT;
import static org.objectweb.asm.Opcodes.ACC_VARARGS;
import static org.objectweb.asm.Opcodes.ACC_VOLATILE;

public enum Access
{
    PUBLIC(ACC_PUBLIC),
    PRIVATE(ACC_PRIVATE),
    PROTECTED(ACC_PROTECTED),
    STATIC(ACC_STATIC),
    FINAL(ACC_FINAL),
    SUPER(ACC_SUPER),
    SYNCHRONIZED(ACC_SYNCHRONIZED),
    VOLATILE(ACC_VOLATILE),
    BRIDGE(ACC_BRIDGE),
    VARARGS(ACC_VARARGS),
    TRANSIENT(ACC_TRANSIENT),
    NATIVE(ACC_NATIVE),
    INTERFACE(ACC_INTERFACE),
    ABSTRACT(ACC_ABSTRACT),
    STRICT(ACC_STRICT),
    SYNTHETIC(ACC_SYNTHETIC),
    ANNOTATION(ACC_ANNOTATION),
    ENUM(ACC_ENUM);

    private final int modifier;

    Access(int modifier)
    {
        this.modifier = modifier;
    }

    public int getModifier()
    {
        return modifier;
    }

    @Override
    public String toString()
    {
        return name().toLowerCase(ENGLISH);
    }

    public static EnumSet<Access> a(Access... access)
    {
        return EnumSet.copyOf(ImmutableList.copyOf(access));
    }

    public static int toAccessModifier(Iterable<Access> accesses)
    {
        int modifier = 0;
        for (Access access : accesses) {
            modifier += access.getModifier();
        }
        return modifier;
    }
}
