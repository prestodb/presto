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
package com.facebook.presto.release.git;

import com.facebook.presto.release.AbstractAnnotatedProvider;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;

import java.lang.annotation.Annotation;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;

// To perform the binding: gitBinder(binder).bind(annotation, repositoryName)
public class GitBinder
{
    private final Binder binder;

    private GitBinder(Binder binder)
    {
        this.binder = requireNonNull(binder, "binder is null");
    }

    public static GitBinder gitBinder(Binder binder)
    {
        return new GitBinder(binder);
    }

    public void bind(Class<? extends Annotation> annotation, String repositoryName)
    {
        configBinder(binder).bindConfig(LocalRepositoryConfig.class, annotation, repositoryName);
        binder.bind(LocalRepository.class).annotatedWith(annotation)
                .toProvider(new LocalRepositoryProvider(annotation, repositoryName));
        binder.bind(Git.class).annotatedWith(annotation)
                .toProvider(new GitProvider(annotation));
    }

    private static final class LocalRepositoryProvider
            extends AbstractAnnotatedProvider<LocalRepository>
    {
        private final String repositoryName;

        public LocalRepositoryProvider(Class<? extends Annotation> annotation, String repositoryName)
        {
            super(annotation);
            this.repositoryName = requireNonNull(repositoryName, "repositoryName is null");
        }

        @Override
        protected LocalRepository get(Injector injector, Class<? extends Annotation> annotation)
        {
            LocalRepositoryConfig config = injector.getInstance(Key.get(LocalRepositoryConfig.class, annotation));
            return new LocalRepository(repositoryName, config);
        }
    }

    private static final class GitProvider
            extends AbstractAnnotatedProvider<Git>
    {
        public GitProvider(Class<? extends Annotation> annotation)
        {
            super(annotation);
        }

        @Override
        protected Git get(Injector injector, Class<? extends Annotation> annotation)
        {
            GitConfig gitConfig = injector.getInstance(GitConfig.class);
            LocalRepository localRepository = injector.getInstance(Key.get(LocalRepository.class, annotation));
            return new GitCommands(localRepository, gitConfig);
        }
    }
}
