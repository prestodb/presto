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

import com.facebook.airlift.configuration.AbstractConfigurationAwareModule;
import com.facebook.presto.release.AbstractAnnotatedProvider;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Singleton;

import java.lang.annotation.Annotation;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;

public class GitRepositoryModule
        extends AbstractConfigurationAwareModule
{
    private final Class<? extends Annotation> annotation;
    private final String repositoryName;

    public GitRepositoryModule(Class<? extends Annotation> annotation, String repositoryName)
    {
        this.annotation = requireNonNull(annotation, "annotation is null");
        this.repositoryName = requireNonNull(repositoryName, "repositoryName is null");
    }

    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(FileRepositoryConfig.class, annotation, repositoryName);

        boolean initializeFromRemote = buildConfigObject(FileRepositoryConfig.class, repositoryName).isInitializeFromRemote();
        if (initializeFromRemote) {
            configBinder(binder).bindConfig(RemoteRepositoryConfig.class, annotation, repositoryName);
        }
        binder.bind(GitRepository.class).annotatedWith(annotation)
                .toProvider(new GitRepositoryProvider(annotation, repositoryName, initializeFromRemote))
                .in(Singleton.class);
        binder.bind(Git.class).annotatedWith(annotation)
                .toProvider(new GitProvider(annotation))
                .in(Singleton.class);
    }

    private static final class GitRepositoryProvider
            extends AbstractAnnotatedProvider<GitRepository>
    {
        private final String repositoryName;
        private final boolean initializeFromRemote;

        public GitRepositoryProvider(Class<? extends Annotation> annotation, String repositoryName, boolean initializeFromRemote)
        {
            super(annotation);
            this.repositoryName = requireNonNull(repositoryName, "repositoryName is null");
            this.initializeFromRemote = initializeFromRemote;
        }

        @Override
        protected GitRepository get(Injector injector, Class<? extends Annotation> annotation)
        {
            FileRepositoryConfig fileConfig = injector.getInstance(Key.get(FileRepositoryConfig.class, annotation));
            if (initializeFromRemote) {
                return GitRepository.fromRemote(
                        repositoryName,
                        fileConfig,
                        injector.getInstance(Key.get(RemoteRepositoryConfig.class, annotation)),
                        injector.getInstance(Key.get(GitConfig.class)));
            }
            return GitRepository.fromFile(repositoryName, fileConfig);
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
            GitRepository repository = injector.getInstance(Key.get(GitRepository.class, annotation));
            return new GitCommands(repository, gitConfig);
        }
    }
}
