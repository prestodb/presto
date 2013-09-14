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
package com.facebook.presto.byteCode;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.tree.ClassNode;
import org.objectweb.asm.util.CheckClassAdapter;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import static com.facebook.presto.byteCode.ParameterizedType.typeFromPathName;

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
        return new ClassInfoLoader(classNodes.build(), ImmutableMap.<ParameterizedType, byte[]>of(), classLoader, true);
    }

    public static ClassInfoLoader createByteCodeInfoLoader(Map<ParameterizedType, byte[]> byteCodes, ClassLoader classLoader, boolean loadMethodNodes)
    {
        return new ClassInfoLoader(ImmutableMap.<ParameterizedType, ClassNode>of(), byteCodes, classLoader, loadMethodNodes);
    }

    private final Map<ParameterizedType, ClassNode> classNodes;
    private final Map<ParameterizedType, byte[]> byteCodes;
    private final ClassLoader classLoader;
    private final Map<ParameterizedType, ClassInfo> classInfoCache = new HashMap<>();
    private final boolean loadMethodNodes;

    public ClassInfoLoader(Map<ParameterizedType, ClassNode> classNodes, Map<ParameterizedType, byte[]> byteCodes, ClassLoader classLoader, boolean loadMethodNodes)
    {
        this.classNodes = ImmutableMap.copyOf(classNodes);
        this.byteCodes = ImmutableMap.copyOf(byteCodes);
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
        byte[] byteCode = byteCodes.get(type);
        if (byteCode != null) {
            classReader = new ClassReader(byteCode);
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
