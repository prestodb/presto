/*
 * Copyright (C) 2012 Facebook, Inc.
 *
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
package com.facebook.presto.functionNamespace.execution.thrift;

import com.google.inject.Injector;
import jakarta.inject.Inject;

import javax.inject.Provider;

import java.lang.annotation.Annotation;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public abstract class AbstractAnnotatedProvider<T>
        implements Provider<T>
{
    private final Annotation annotation;
    private Injector injector;

    protected AbstractAnnotatedProvider(Annotation annotation)
    {
        this.annotation = requireNonNull(annotation, "annotation is null");
    }

    @Inject
    public final void setInjector(Injector injector)
    {
        this.injector = injector;
    }

    @Override
    public final T get()
    {
        checkState(injector != null, "injector was not set");
        return get(injector, annotation);
    }

    protected abstract T get(Injector injector, Annotation annotation);

    @Override
    public final boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        AbstractAnnotatedProvider<?> other = (AbstractAnnotatedProvider<?>) obj;
        return Objects.equals(this.annotation, other.annotation);
    }

    @Override
    public final int hashCode()
    {
        return Objects.hash(annotation);
    }
}
