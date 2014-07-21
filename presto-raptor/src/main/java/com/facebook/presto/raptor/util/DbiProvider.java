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
package com.facebook.presto.raptor.util;

import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import javax.inject.Inject;
import javax.sql.DataSource;

import java.lang.annotation.Annotation;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.inject.multibindings.Multibinder.newSetBinder;

public class DbiProvider
        implements Provider<IDBI>
{
    private final Class<? extends Annotation> annotationType;
    private Injector injector;

    public DbiProvider(Class<? extends Annotation> annotationType)
    {
        this.annotationType = checkNotNull(annotationType, "annotationType is null");
    }

    @Inject
    public void setInjector(Injector injector)
    {
        this.injector = checkNotNull(injector, "injector is null");
    }

    @Override
    public IDBI get()
    {
        checkState(injector != null, "injector was not set");
        DBI dbi = new DBI(injector.getInstance(Key.get(DataSource.class, annotationType)));
        Key<Set<ResultSetMapper<?>>> mappersKey = Key.get(new TypeLiteral<Set<ResultSetMapper<?>>>() {}, annotationType);
        if (injector.getExistingBinding(mappersKey) != null) {
            for (ResultSetMapper<?> mapper : injector.getInstance(mappersKey)) {
                dbi.registerMapper(mapper);
            }
        }
        return dbi;
    }

    public static void bindDbiToDataSource(Binder bind, Class<? extends Annotation> annotationType)
    {
        bind.bind(IDBI.class).annotatedWith(annotationType).toProvider(new DbiProvider(annotationType)).in(Scopes.SINGLETON);
    }

    public static void bindResultSetMapper(Binder binder, Class<? extends ResultSetMapper<?>> mapper, Class<? extends Annotation> annotation)
    {
        newSetBinder(binder, new TypeLiteral<ResultSetMapper<?>>() {}, annotation)
                .addBinding().to(mapper).in(Scopes.SINGLETON);
    }
}
