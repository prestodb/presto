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

import org.objectweb.asm.ClassWriter;

import static com.facebook.presto.bytecode.ParameterizedType.typeFromPathName;

public class SmartClassWriter
        extends ClassWriter
{
    private final ClassInfoLoader classInfoLoader;

    public SmartClassWriter(ClassInfoLoader classInfoLoader)
    {
        super(ClassWriter.COMPUTE_FRAMES);
        this.classInfoLoader = classInfoLoader;
    }

    @Override
    protected String getCommonSuperClass(String aType, String bType)
    {
        ClassInfo aClassInfo = classInfoLoader.loadClassInfo(typeFromPathName(aType));
        ClassInfo bClassInfo = classInfoLoader.loadClassInfo(typeFromPathName(bType));

        if (aClassInfo.isAssignableFrom(bClassInfo)) {
            return aType;
        }
        if (bClassInfo.isAssignableFrom(aClassInfo)) {
            return bType;
        }
        if (aClassInfo.isInterface() || bClassInfo.isInterface()) {
            return "java/lang/Object";
        }
        else {
            do {
                aClassInfo = aClassInfo.getSuperclass();
            }
            while (!aClassInfo.isAssignableFrom(bClassInfo));
            return aClassInfo.getType().getClassName();
        }
    }
}
