package com.facebook.presto.byteCode;

import org.objectweb.asm.ClassWriter;

import static com.facebook.presto.byteCode.ParameterizedType.typeFromPathName;

public class SmartClassWriter extends ClassWriter
{
    private final ClassInfoLoader classInfoLoader;

    public SmartClassWriter(ClassInfoLoader classInfoLoader)
    {
        super(ClassWriter.COMPUTE_FRAMES);
        this.classInfoLoader = classInfoLoader;
    }

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
            } while (!aClassInfo.isAssignableFrom(bClassInfo));
            return aClassInfo.getType().getClassName();
        }
    }
}
