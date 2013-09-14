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
package com.facebook.presto.guice;

import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Provider;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.IDBI;

import javax.inject.Inject;
import javax.sql.DataSource;

import java.lang.annotation.Annotation;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

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
        return new DBI(injector.getInstance(Key.get(DataSource.class, annotationType)));
    }

    public static void bindDbiToDataSource(Binder bind, Class<? extends Annotation> annotationType)
    {
        bind.bind(IDBI.class).annotatedWith(annotationType).toProvider(new DbiProvider(annotationType));
    }
}
