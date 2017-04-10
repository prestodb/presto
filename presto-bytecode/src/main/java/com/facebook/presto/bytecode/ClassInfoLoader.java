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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.tree.ClassNode;
import org.objectweb.asm.util.CheckClassAdapter;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import static com.facebook.presto.bytecode.ParameterizedType.typeFromPathName;

public class ClassInfoLoader
{
    public static ClassInfoLoader createClassInfoLoader(Iterable<ClassDefinition> classDefinitions, ClassLoader classLoader)
    {
        ImmutableMap.Builder<ParameterizedType, ClassNode> classNodes = ImmutableMap.builder();
        for (ClassDefinition classDefinition : classDefinitions) {
            ClassNode classNode = new ClassNode();
            classDefinition.visit(classNode);
            classNodes.put(classDefinition.getType(), classNode);
        }
        return new ClassInfoLoader(classNodes.build(), ImmutableMap.of(), classLoader, true);
    }

    private final Map<ParameterizedType, ClassNode> classNodes;
    private final Map<ParameterizedType, byte[]> bytecodes;
    private final ClassLoader classLoader;
    private final Map<ParameterizedType, ClassInfo> classInfoCache = new HashMap<>();
    private final boolean loadMethodNodes;

    public ClassInfoLoader(Map<ParameterizedType, ClassNode> classNodes, Map<ParameterizedType, byte[]> bytecodes, ClassLoader classLoader, boolean loadMethodNodes)
    {
        this.classNodes = ImmutableMap.copyOf(classNodes);
        this.bytecodes = ImmutableMap.copyOf(bytecodes);
        this.classLoader = classLoader;
        this.loadMethodNodes = loadMethodNodes;
    }

    public ClassInfo loadClassInfo(ParameterizedType type)
    {
        ClassInfo classInfo = classInfoCache.get(type);
        if (classInfo == null) {
            classInfo = readClassInfoQuick(type);
            classInfoCache.put(type, classInfo);
        }
        return classInfo;
    }

    private ClassInfo readClassInfoQuick(ParameterizedType type)
    {
        // check for user supplied class node
        ClassNode classNode = classNodes.get(type);
        if (classNode != null) {
            return new ClassInfo(this, classNode);
        }

        // check for user supplied byte code
        ClassReader classReader;
        byte[] bytecode = bytecodes.get(type);
        if (bytecode != null) {
            classReader = new ClassReader(bytecode);
        }
        else {
            // load class file from class loader
            String classFileName = type.getClassName() + ".class";
            try (InputStream is = classLoader.getResourceAsStream(classFileName)) {
                classReader = new ClassReader(is);
            }
            catch (IOException e) {
                // check if class is already loaded
                try {
                    Class<?> aClass = classLoader.loadClass(type.getJavaClassName());
                    return new ClassInfo(this, aClass);
                }
                catch (ClassNotFoundException e1) {
                    throw new RuntimeException("Class not found " + type, e);
                }
            }
        }

        if (loadMethodNodes) {
            // slower version that loads all operations
            classNode = new ClassNode();
            classReader.accept(new CheckClassAdapter(classNode, false), ClassReader.SKIP_DEBUG);

            return new ClassInfo(this, classNode);
        }
        else {
            // optimized version
            int header = classReader.header;
            int access = classReader.readUnsignedShort(header);

            char[] buf = new char[2048];

            // read super class name
            int superClassIndex = classReader.getItem(classReader.readUnsignedShort(header + 4));
            ParameterizedType superClass;
            if (superClassIndex == 0) {
                superClass = null;
            }
            else {
                superClass = typeFromPathName(classReader.readUTF8(superClassIndex, buf));
            }

            // read each interface name
            int interfaceCount = classReader.readUnsignedShort(header + 6);
            ImmutableList.Builder<ParameterizedType> interfaces = ImmutableList.builder();
            header += 8;
            for (int i = 0; i < interfaceCount; ++i) {
                interfaces.add(typeFromPathName(classReader.readClass(header, buf)));
                header += 2;
            }
            return new ClassInfo(this, type, access, superClass, interfaces.build(), null);
        }
    }
}
