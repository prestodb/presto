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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Primitives;
import org.objectweb.asm.AnnotationVisitor;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.FieldVisitor;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Type;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static com.facebook.presto.bytecode.ParameterizedType.type;
import static java.util.Objects.requireNonNull;

public class AnnotationDefinition
{
    private static final Set<Class<?>> ALLOWED_TYPES = ImmutableSet.<Class<?>>builder()
            .addAll(Primitives.allWrapperTypes())
            .add(String.class)
            .add(Class.class)
            .add(ParameterizedType.class)
            .add(AnnotationDefinition.class)
            .add(Enum.class)
            .build();

    private final ParameterizedType type;
    private final Map<String, Object> values = new LinkedHashMap<>();

    public AnnotationDefinition(Class<?> type)
    {
        this.type = type(type);
    }

    public AnnotationDefinition(ParameterizedType type)
    {
        this.type = type;
    }

    public AnnotationDefinition setValue(String name, Byte value)
    {
        return setValueInternal(name, value);
    }

    public AnnotationDefinition setValue(String name, Boolean value)
    {
        return setValueInternal(name, value);
    }

    public AnnotationDefinition setValue(String name, Character value)
    {
        return setValueInternal(name, value);
    }

    public AnnotationDefinition setValue(String name, Short value)
    {
        return setValueInternal(name, value);
    }

    public AnnotationDefinition setValue(String name, Integer value)
    {
        return setValueInternal(name, value);
    }

    public AnnotationDefinition setValue(String name, Long value)
    {
        return setValueInternal(name, value);
    }

    public AnnotationDefinition setValue(String name, Float value)
    {
        return setValueInternal(name, value);
    }

    public AnnotationDefinition setValue(String name, Double value)
    {
        return setValueInternal(name, value);
    }

    public AnnotationDefinition setValue(String name, String value)
    {
        return setValueInternal(name, value);
    }

    public AnnotationDefinition setValue(String name, Class<?> value)
    {
        return setValueInternal(name, value);
    }

    public AnnotationDefinition setValue(String name, ParameterizedType value)
    {
        return setValueInternal(name, value);
    }

    public AnnotationDefinition setValue(String name, AnnotationDefinition value)
    {
        return setValueInternal(name, value);
    }

    public AnnotationDefinition setValue(String name, Enum<?> value)
    {
        return setValueInternal(name, value);
    }

    public AnnotationDefinition setValue(String name, List<?> value)
    {
        return setValueInternal(name, value);
    }

    private AnnotationDefinition setValueInternal(String name, Object value)
    {
        requireNonNull(name, "name is null");
        requireNonNull(value, "value is null");

        isValidType(value);

        values.put(name, value);
        return this;
    }

    public ParameterizedType getType()
    {
        return type;
    }

    public Map<String, Object> getValues()
    {
        // todo we need an unmodifiable view
        return values;
    }

    private static void isValidType(Object value)
    {
        if (value instanceof List) {
            // todo verify list contains single type
            for (Object v : (List<Object>) value) {
                Preconditions.checkArgument(ALLOWED_TYPES.contains(v.getClass()), "List contains invalid type %s", v.getClass());
                if (v instanceof List) {
                    isValidType(value);
                }
            }
        }
        else {
            Preconditions.checkArgument(ALLOWED_TYPES.contains(value.getClass()), "Invalid value type %s", value.getClass());
        }
    }

    public void visitClassAnnotation(ClassVisitor visitor)
    {
        AnnotationVisitor annotationVisitor = visitor.visitAnnotation(type.getType(), true);
        visit(annotationVisitor);
        annotationVisitor.visitEnd();
    }

    public void visitFieldAnnotation(FieldVisitor visitor)
    {
        AnnotationVisitor annotationVisitor = visitor.visitAnnotation(type.getType(), true);
        visit(annotationVisitor);
        annotationVisitor.visitEnd();
    }

    public void visitMethodAnnotation(MethodVisitor visitor)
    {
        AnnotationVisitor annotationVisitor = visitor.visitAnnotation(type.getType(), true);
        visit(annotationVisitor);
        annotationVisitor.visitEnd();
    }

    public void visitParameterAnnotation(int parameterIndex, MethodVisitor visitor)
    {
        AnnotationVisitor annotationVisitor = visitor.visitParameterAnnotation(parameterIndex, type.getType(), true);
        visit(annotationVisitor);
        annotationVisitor.visitEnd();
    }

    private void visit(AnnotationVisitor visitor)
    {
        for (Entry<String, Object> entry : values.entrySet()) {
            String name = entry.getKey();
            Object value = entry.getValue();
            visit(visitor, name, value);
        }
    }

    private static void visit(AnnotationVisitor visitor, String name, Object value)
    {
        if (value instanceof AnnotationDefinition) {
            AnnotationDefinition annotation = (AnnotationDefinition) value;
            AnnotationVisitor annotationVisitor = visitor.visitAnnotation(name, annotation.type.getType());
            annotation.visit(annotationVisitor);
        }
        else if (value instanceof Enum) {
            Enum<?> enumConstant = (Enum<?>) value;
            visitor.visitEnum(name, type(enumConstant.getDeclaringClass()).getClassName(), enumConstant.name());
        }
        else if (value instanceof ParameterizedType) {
            ParameterizedType parameterizedType = (ParameterizedType) value;
            visitor.visit(name, Type.getType(parameterizedType.getType()));
        }
        else if (value instanceof Class) {
            Class<?> clazz = (Class<?>) value;
            visitor.visit(name, Type.getType(clazz));
        }
        else if (value instanceof List) {
            AnnotationVisitor arrayVisitor = visitor.visitArray(name);
            for (Object element : (List<?>) value) {
                visit(arrayVisitor, null, element);
            }
            arrayVisitor.visitEnd();
        }
        else {
            visitor.visit(name, value);
        }
    }
}
