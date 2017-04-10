/***
 * ASM tests
 * Copyright (c) 2002-2005 France Telecom
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. Neither the name of the copyright holders nor the names of its
 *    contributors may be used to endorse or promote products derived from
 *    this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
 * THE POSSIBILITY OF SUCH DAMAGE.
 */
package com.facebook.presto.bytecode;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.tree.ClassNode;
import org.objectweb.asm.tree.MethodNode;

import java.util.List;

import static com.facebook.presto.bytecode.ParameterizedType.type;
import static com.facebook.presto.bytecode.ParameterizedType.typeFromPathName;
import static com.google.common.collect.Iterables.transform;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;

/**
 * @author Eugene Kuleshov
 */
public class ClassInfo
{
    private final ClassInfoLoader loader;
    private final ParameterizedType type;
    private final int access;
    private final ParameterizedType superClass;
    private final List<ParameterizedType> interfaces;
    private final List<MethodNode> methods;

    public ClassInfo(ClassInfoLoader loader, ClassNode classNode)
    {
        this(loader,
                typeFromPathName(classNode.name),
                classNode.access,
                classNode.superName == null ? null : typeFromPathName(classNode.superName),
                transform((List<String>) classNode.interfaces, ParameterizedType::typeFromPathName),
                (List<MethodNode>) classNode.methods);
    }

    public ClassInfo(ClassInfoLoader loader, Class<?> aClass)
    {
        this(loader,
                type(aClass),
                aClass.getModifiers(),
                aClass.getSuperclass() == null ? null : type(aClass.getSuperclass()),
                transform(asList(aClass.getInterfaces()), ParameterizedType::type),
                null);
    }

    public ClassInfo(ClassInfoLoader loader, ParameterizedType type, int access, ParameterizedType superClass, Iterable<ParameterizedType> interfaces, Iterable<MethodNode> methods)
    {
        requireNonNull(loader, "loader is null");
        requireNonNull(type, "type is null");
        requireNonNull(interfaces, "interfaces is null");

        this.loader = loader;
        this.type = type;
        this.access = access;
        this.superClass = superClass;
        this.interfaces = ImmutableList.copyOf(interfaces);
        if (methods != null) {
            this.methods = ImmutableList.copyOf(methods);
        }
        else {
            this.methods = null;
        }
    }

    public ParameterizedType getType()
    {
        return type;
    }

    public int getModifiers()
    {
        return access;
    }

    public ClassInfo getSuperclass()
    {
        if (superClass == null) {
            return null;
        }
        return loader.loadClassInfo(superClass);
    }

    public List<ClassInfo> getInterfaces()
    {
        if (interfaces == null) {
            return ImmutableList.of();
        }
        ImmutableList.Builder<ClassInfo> builder = ImmutableList.builder();
        for (ParameterizedType anInterface : interfaces) {
            builder.add(loader.loadClassInfo(anInterface));
        }
        return builder.build();
    }

    public List<MethodNode> getMethods()
    {
        Preconditions.checkState(methods != null, "Methods were not loaded for type %s", type);
        return methods;
    }

    boolean isInterface()
    {
        return (getModifiers() & Opcodes.ACC_INTERFACE) > 0;
    }

    private boolean implementsInterface(ClassInfo that)
    {
        for (ClassInfo classInfo = this; classInfo != null; classInfo = classInfo.getSuperclass()) {
            for (ClassInfo anInterface : classInfo.getInterfaces()) {
                if (anInterface.type.equals(that.type) || anInterface.implementsInterface(that)) {
                    return true;
                }
            }
        }
        return false;
    }

    private boolean isSubclassOf(ClassInfo that)
    {
        for (ClassInfo classInfo = this; classInfo != null; classInfo = classInfo.getSuperclass()) {
            if (classInfo.getSuperclass() != null &&
                    classInfo.getSuperclass().type.equals(that.type)) {
                return true;
            }
        }
        return false;
    }

    public boolean isAssignableFrom(ClassInfo that)
    {
        if (this == that) {
            return true;
        }

        if (that.isSubclassOf(this)) {
            return true;
        }

        if (that.implementsInterface(this)) {
            return true;
        }

        if (that.isInterface() && getType().equals(type(Object.class))) {
            return true;
        }

        return false;
    }

    @Override
    public String toString()
    {
        return type.toString();
    }
}
