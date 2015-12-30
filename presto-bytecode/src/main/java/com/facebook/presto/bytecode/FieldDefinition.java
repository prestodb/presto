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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.FieldVisitor;

import javax.annotation.concurrent.Immutable;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

import static com.facebook.presto.bytecode.Access.toAccessModifier;
import static com.facebook.presto.bytecode.ParameterizedType.type;

@SuppressWarnings("UnusedDeclaration")
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
        StringBuilder sb = new StringBuilder();
        sb.append("FieldDefinition");
        sb.append("{access=").append(access);
        sb.append(", name='").append(name).append('\'');
        sb.append(", type=").append(type);
        sb.append('}');
        return sb.toString();
    }
}
