/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.byteCode;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.FieldVisitor;

import javax.annotation.concurrent.Immutable;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

import static com.facebook.presto.byteCode.Access.toAccessModifier;
import static com.facebook.presto.byteCode.ParameterizedType.type;

@Immutable
public class FieldDefinition
{
    private final ClassDefinition declaringClass;
    private final ImmutableSet<Access> access;
    private final String name;
    private final ParameterizedType type;
    private final List<AnnotationDefinition> annotations = new ArrayList<>();

    public FieldDefinition(ClassDefinition declaringClass, EnumSet<Access> access, String name, ParameterizedType type)
    {
        this.declaringClass = declaringClass;
        this.access = Sets.immutableEnumSet(access);
        this.name = name;
        this.type = type;
    }

    public FieldDefinition(ClassDefinition declaringClass, EnumSet<Access> access, String name, Class<?> type)
    {
        this(declaringClass, access, name, type(type));
    }

    public ClassDefinition getDeclaringClass()
    {
        return declaringClass;
    }

    public ImmutableSet<Access> getAccess()
    {
        return access;
    }

    public String getName()
    {
        return name;
    }

    public ParameterizedType getType()
    {
        return type;
    }

    public List<AnnotationDefinition> getAnnotations()
    {
        return ImmutableList.copyOf(annotations);
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


    public void visit(ClassVisitor visitor)
    {
        FieldVisitor fieldVisitor = visitor.visitField(toAccessModifier(access),
                name,
                type.getType(),
                type.getGenericSignature(),
                null);

        if (fieldVisitor == null) {
            return;
        }

        for (AnnotationDefinition annotation : annotations) {
            annotation.visitFieldAnnotation(fieldVisitor);
        }

        fieldVisitor.visitEnd();
    }

    @Override
    public String toString()
    {
        final StringBuilder sb = new StringBuilder();
        sb.append("FieldDefinition");
        sb.append("{access=").append(access);
        sb.append(", name='").append(name).append('\'');
        sb.append(", type=").append(type);
        sb.append('}');
        return sb.toString();
    }
}
