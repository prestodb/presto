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
package com.facebook.presto.release.maven;

import com.facebook.presto.release.AbstractAnnotatedProvider;
import com.facebook.presto.release.git.GitRepository;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Singleton;

import java.lang.annotation.Annotation;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;

public class MavenModule
        implements Module
{
    private final Class<? extends Annotation> annotation;

    public MavenModule(Class<? extends Annotation> annotation)
    {
        this.annotation = requireNonNull(annotation, "annotation is null");
    }

    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(MavenConfig.class);
        binder.bind(Maven.class).annotatedWith(annotation)
                .toProvider(new MavenProvider(annotation))
                .in(Singleton.class);
    }

    private static final class MavenProvider
            extends AbstractAnnotatedProvider<Maven>
    {
        public MavenProvider(Class<? extends Annotation> annotation)
        {
            super(annotation);
        }

        @Override
        protected Maven get(Injector injector, Class<? extends Annotation> annotation)
        {
            MavenConfig mavenConfig = injector.getInstance(MavenConfig.class);
            GitRepository repository = injector.getInstance(Key.get(GitRepository.class, annotation));
            return new MavenCommands(mavenConfig.getExecutable(), repository.getDirectory());
        }
    }
}
