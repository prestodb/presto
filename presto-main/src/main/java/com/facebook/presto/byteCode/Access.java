/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.byteCode;

import com.google.common.collect.ImmutableList;

import java.util.EnumSet;

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

    private int modifier;

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
        return super.name().toLowerCase();
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

