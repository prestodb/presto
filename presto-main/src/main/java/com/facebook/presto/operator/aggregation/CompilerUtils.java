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
package com.facebook.presto.operator.aggregation;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

import static com.google.common.base.Preconditions.checkArgument;

public final class CompilerUtils
{
    private CompilerUtils() {}

    public static Method findPublicStaticMethodWithAnnotation(Class<?> clazz, Class<?> annotationClass)
    {
        Method ret = null;
        for (Method method : clazz.getMethods()) {
            for (Annotation annotation : method.getAnnotations()) {
                if (annotationClass.isInstance(annotation)) {
                    checkArgument(Modifier.isStatic(method.getModifiers()) && Modifier.isPublic(method.getModifiers()), "%s annotated with %s must be public and static", method.getName(), annotationClass.getSimpleName());
                    checkArgument(ret == null, "Methods %s and %s both annotated with %s", ret, method, annotationClass);
                    ret = method;
                    break;
                }
            }
        }
        return ret;
    }
}
