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

import com.facebook.presto.byteCode.debug.LocalVariableNode;
import com.facebook.presto.byteCode.instruction.LabelNode;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.tree.InsnNode;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

import static com.facebook.presto.byteCode.Access.STATIC;
import static com.facebook.presto.byteCode.Access.toAccessModifier;
import static com.facebook.presto.byteCode.NamedParameterDefinition.getNamedParameterType;
import static com.facebook.presto.byteCode.ParameterizedType.getParameterType;
import static com.facebook.presto.byteCode.ParameterizedType.toParameterizedType;
import static com.facebook.presto.byteCode.ParameterizedType.type;
import static com.google.common.collect.Iterables.transform;
import static org.objectweb.asm.Opcodes.RETURN;

@NotThreadSafe
public class MethodDefinition
{
    private final CompilerContext compilerContext;
    private final ClassDefinition declaringClass;
    private final EnumSet<Access> access;
    private final String name;
    private final List<AnnotationDefinition> annotations = new ArrayList<>();
    private final ParameterizedType returnType;
    private final List<NamedParameterDefinition> parameters;
    private final List<ParameterizedType> parameterTypes;
    private final List<List<AnnotationDefinition>> parameterAnnotations;
    private final List<ParameterizedType> exceptions = new ArrayList<>();
    private final List<LocalVariableNode> localVariableNodes = new ArrayList<>();

    private final Block body;
    private String comment;

    public MethodDefinition(
            CompilerContext compilerContext, ClassDefinition declaringClass,
            EnumSet<Access> access,
            String name,
            ParameterizedType returnType,
            NamedParameterDefinition... parameters
    )
    {
        this(compilerContext, declaringClass, access, name, returnType, ImmutableList.copyOf(parameters));
    }

    public MethodDefinition(
            CompilerContext compilerContext,
            ClassDefinition declaringClass,
            EnumSet<Access> access,
            String name,
            ParameterizedType returnType,
            Iterable<NamedParameterDefinition> parameters
    )
    {
        this.compilerContext = compilerContext;
        this.declaringClass = declaringClass;
        body = new Block(compilerContext);

        this.access = access;
        this.name = name;
        if (returnType != null) {
            this.returnType = returnType;
        }
        else {
            this.returnType = type(void.class);
        }
        this.parameters = ImmutableList.copyOf(parameters);
        this.parameterTypes = Lists.transform(this.parameters, getNamedParameterType());
        this.parameterAnnotations = ImmutableList.copyOf(Iterables.transform(parameters, new Function<NamedParameterDefinition, List<AnnotationDefinition>>()
        {
            @Override
            public List<AnnotationDefinition> apply(@Nullable NamedParameterDefinition input)
            {
                return new ArrayList<>();
            }
        }));

        if (!access.contains(STATIC)) {
            getCompilerContext().declareThisVariable(type(Object.class));
        }
        int argId = 0;
        for (NamedParameterDefinition parameter : parameters) {
            String parameterName = parameter.getName();
            if (parameterName == null) {
                parameterName = "arg" + argId;
            }
            getCompilerContext().declareParameter(parameter.getType(), parameterName);
            argId++;
        }
    }

    public ClassDefinition getDeclaringClass()
    {
        return declaringClass;
    }

    public List<AnnotationDefinition> getAnnotations()
    {
        return ImmutableList.copyOf(annotations);
    }

    public List<AnnotationDefinition> getParameterAnnotations(int index)
    {
        return ImmutableList.copyOf(parameterAnnotations.get(index));
    }

    public EnumSet<Access> getAccess()
    {
        return access;
    }

    public String getName()
    {
        return name;
    }

    public ParameterizedType getReturnType()
    {
        return returnType;
    }

    public List<NamedParameterDefinition> getParameters()
    {
        return parameters;
    }

    public List<ParameterizedType> getParameterTypes()
    {
        return parameterTypes;
    }

    public List<ParameterizedType> getExceptions()
    {
        return exceptions;
    }

    public MethodDefinition addException(Class<? extends Throwable> exceptionClass)
    {
        exceptions.add(type(exceptionClass));
        return this;
    }

    public MethodDefinition comment(String format, Object... args)
    {
        this.comment = String.format(format, args);
        return this;
    }

    public String getComment()
    {
        return comment;
    }

    public CompilerContext getCompilerContext()
    {
        return compilerContext;
    }

    public String getMethodDescriptor()
    {
        return methodDescription(returnType, parameterTypes);
    }

    public Block getBody()
    {
        return body;
    }

    public AnnotationDefinition declareAnnotation(Class<?> type)
    {
        AnnotationDefinition annotationDefinition = new AnnotationDefinition(type);
        annotations.add(annotationDefinition);
        return annotationDefinition;
    }

    public AnnotationDefinition declareAnnotation(ParameterizedType type)
    {
        AnnotationDefinition annotationDefinition = new AnnotationDefinition(type);
        annotations.add(annotationDefinition);
        return annotationDefinition;
    }

    public AnnotationDefinition declareParameterAnnotation(Class<?> type, int parameterIndex)
    {
        AnnotationDefinition annotationDefinition = new AnnotationDefinition(type);
        parameterAnnotations.get(parameterIndex).add(annotationDefinition);
        return annotationDefinition;
    }

    public AnnotationDefinition declareParameterAnnotation(ParameterizedType type, int parameterIndex)
    {
        AnnotationDefinition annotationDefinition = new AnnotationDefinition(type);
        parameterAnnotations.get(parameterIndex).add(annotationDefinition);
        return annotationDefinition;
    }

    public void visit(ClassVisitor visitor)
    {
        visit(visitor, false);
    }

    public void visit(ClassVisitor visitor, boolean addReturn)
    {
        String[] exceptions = new String[this.exceptions.size()];
        for (int i = 0; i < exceptions.length; i++) {
            exceptions[i] = this.exceptions.get(i).getClassName();
        }

        MethodVisitor methodVisitor = visitor.visitMethod(toAccessModifier(access),
                name,
                getMethodDescriptor(),
                genericMethodSignature(returnType, parameterTypes),
                exceptions);

        if (methodVisitor == null) {
            return;
        }

        // visit method annotations
        for (AnnotationDefinition annotation : annotations) {
            annotation.visitMethodAnnotation(methodVisitor);
        }

        // visit parameter annotations
        for (int parameterIndex = 0; parameterIndex < parameterAnnotations.size(); parameterIndex++) {
            List<AnnotationDefinition> parameterAnnotations1 = this.parameterAnnotations.get(parameterIndex);
            for (AnnotationDefinition parameterAnnotation : parameterAnnotations1) {
                parameterAnnotation.visitParameterAnnotation(parameterIndex, methodVisitor);
            }
        }

        // visit code
        methodVisitor.visitCode();

//        // visit try catch blocks
//        for (TryCatchBlockNode tryCatchBlockNode : body.getTryCatchBlocks()) {
//            tryCatchBlockNode.accept(methodVisitor);
//        }

        // visit instructions
        body.accept(methodVisitor);
        if (addReturn) {
            new InsnNode(RETURN).accept(methodVisitor);
        }

        // visit local variable declarations
        for (LocalVariableNode localVariableNode : localVariableNodes) {
            localVariableNode.accept(methodVisitor);
        }

        // done
        methodVisitor.visitMaxs(-1, -1);
        methodVisitor.visitEnd();
    }

    public static String methodDescription(Class<?> returnType, Class<?>... parameterTypes)
    {
        return methodDescription(returnType, ImmutableList.copyOf(parameterTypes));
    }

    public static String methodDescription(Class<?> returnType, List<Class<?>> parameterTypes)
    {
        return methodDescription(
                type(returnType),
                Lists.transform(parameterTypes, toParameterizedType())
        );
    }

    public static String methodDescription(
            ParameterizedType returnType,
            ParameterizedType... parameterTypes
    )
    {
        return methodDescription(returnType, ImmutableList.copyOf(parameterTypes));
    }

    public static String methodDescription(
            ParameterizedType returnType,
            List<ParameterizedType> parameterTypes
    )
    {
        StringBuilder sb = new StringBuilder();
        sb.append("(");
        Joiner.on("").appendTo(sb, transform(parameterTypes, getParameterType()));
        sb.append(")");
        sb.append(returnType.getType());
        return sb.toString();
    }

    public static String genericMethodSignature(
            ParameterizedType returnType,
            ParameterizedType... parameterTypes
    )
    {
        return genericMethodSignature(returnType, ImmutableList.copyOf(parameterTypes));
    }

    public static String genericMethodSignature(
            ParameterizedType returnType,
            List<ParameterizedType> parameterTypes
    )
    {
        StringBuilder sb = new StringBuilder();
        sb.append("(");
        Joiner.on("").appendTo(sb, parameterTypes);
        sb.append(")");
        sb.append(returnType);
        return sb.toString();
    }

    public void addLocalVariable(LocalVariableDefinition localVariable, LabelNode start, LabelNode end)
    {
        localVariableNodes.add(new LocalVariableNode(localVariable, start, end));
    }
}
