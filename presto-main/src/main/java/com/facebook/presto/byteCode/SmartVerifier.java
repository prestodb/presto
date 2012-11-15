package com.facebook.presto.byteCode;

import org.objectweb.asm.Type;
import org.objectweb.asm.tree.MethodNode;
import org.objectweb.asm.tree.analysis.Analyzer;
import org.objectweb.asm.tree.analysis.BasicValue;
import org.objectweb.asm.tree.analysis.Frame;
import org.objectweb.asm.tree.analysis.SimpleVerifier;
import org.objectweb.asm.util.Textifier;
import org.objectweb.asm.util.TraceMethodVisitor;

import java.io.PrintWriter;
import java.util.Map;

import static com.facebook.presto.byteCode.ClassInfoLoader.createByteCodeInfoLoader;
import static com.facebook.presto.byteCode.ParameterizedType.type;

public class SmartVerifier extends SimpleVerifier
{
    public static void verifyClasses(Map<ParameterizedType, byte[]> byteCodes, ClassLoader classLoader, boolean dump, PrintWriter writer)
    {
        ClassInfoLoader classInfoLoader = createByteCodeInfoLoader(byteCodes, classLoader, true);
        for (ParameterizedType type : byteCodes.keySet()) {
            ClassInfo classInfo = classInfoLoader.loadClassInfo(type);

            for (MethodNode method : classInfo.getMethods()) {
                SimpleVerifier verifier = new SmartVerifier(classInfoLoader);
                Analyzer<BasicValue> analyzer = new Analyzer<>(verifier);
                if (classLoader != null) {
                    verifier.setClassLoader(classLoader);
                }
                try {
                    analyzer.analyze(classInfo.getType().getClassName(), method);
                    if (!dump) {
                        continue;
                    }
                }
                catch (Exception e) {
                    e.printStackTrace(writer);
                }
                printAnalyzerResult(method, analyzer, writer);
            }
        }
        writer.flush();
    }

    private final ClassInfoLoader classInfoLoader;

    public SmartVerifier(ClassInfoLoader classInfoLoader)
    {
        this.classInfoLoader = classInfoLoader;
    }

    protected boolean isInterface(Type type)
    {
        ClassInfo classInfo = classInfoLoader.loadClassInfo(type(type));
        if (classInfo != null) {
            return classInfo.isInterface();
        }
        return getClass(type).isInterface();
    }

    protected Type getSuperClass(Type type)
    {
        ClassInfo classInfo = classInfoLoader.loadClassInfo(type(type));
        if (classInfo != null) {
            return classInfo.getSuperclass().getType().getAsmType();
        }
        Class<?> c = getClass(type).getSuperclass();
        return c == null ? null : Type.getType(c);
    }

    protected boolean isAssignableFrom(Type baseType, Type testType)
    {
        if (baseType.equals(testType)) {
            return true;
        }

        ClassInfo baseClassInfo = classInfoLoader.loadClassInfo(type(baseType));
        ClassInfo testClassInfo = classInfoLoader.loadClassInfo(type(testType));
        return baseClassInfo.isAssignableFrom(testClassInfo);
    }

    @Override
    protected Class<?> getClass(Type type)
    {
        return super.getClass(type);
    }

    private static void printAnalyzerResult(MethodNode method, Analyzer<BasicValue> analyzer, PrintWriter writer)
    {
        Frame<BasicValue>[] frames = analyzer.getFrames();
        Textifier t = new Textifier();
        TraceMethodVisitor mv = new TraceMethodVisitor(t);

        writer.println(method.name + method.desc);
        for (int j = 0; j < method.instructions.size(); ++j) {
            method.instructions.get(j).accept(mv);

            StringBuilder stringBuilder = new StringBuilder();
            Frame<BasicValue> f = frames[j];
            if (f == null) {
                stringBuilder.append('?');
            }
            else {
                for (int k = 0; k < f.getLocals(); ++k) {
                    stringBuilder.append(getShortName(f.getLocal(k).toString()))
                            .append(' ');
                }
                stringBuilder.append(" : ");
                for (int k = 0; k < f.getStackSize(); ++k) {
                    stringBuilder.append(getShortName(f.getStack(k).toString()))
                            .append(' ');
                }
            }
            while (stringBuilder.length() < method.maxStack + method.maxLocals + 1) {
                stringBuilder.append(' ');
            }
            writer.print(Integer.toString(j + 100000).substring(1));
            writer.print(" " + stringBuilder + " : " + t.text.get(t.text.size() - 1));
        }
        for (int j = 0; j < method.tryCatchBlocks.size(); ++j) {
            method.tryCatchBlocks.get(j).accept(mv);
            writer.print(" " + t.text.get(t.text.size() - 1));
        }
        writer.println();
    }

    private static String getShortName(String name)
    {
        int n = name.lastIndexOf('/');
        int k = name.length();
        if (name.charAt(k - 1) == ';') {
            k--;
        }
        return n == -1 ? name : name.substring(n + 1, k);
    }
}
