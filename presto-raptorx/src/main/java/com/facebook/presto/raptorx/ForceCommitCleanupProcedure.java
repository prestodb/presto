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
package com.facebook.presto.raptorx;

import com.facebook.presto.raptorx.metadata.CommitCleaner;
import com.facebook.presto.spi.procedure.Procedure;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;
import javax.inject.Provider;

import static com.facebook.presto.raptorx.util.Reflection.getMethod;
import static java.util.Objects.requireNonNull;

public class ForceCommitCleanupProcedure
        implements Provider<Procedure>
{
    private final CommitCleaner cleaner;

    @Inject
    public ForceCommitCleanupProcedure(CommitCleaner cleaner)
    {
        this.cleaner = requireNonNull(cleaner, "cleaner is null");
    }

    @Override
    public Procedure get()
    {
        return new Procedure(
                "system",
                "force_commit_cleanup",
                ImmutableList.of(),
                getMethod(getClass(), "forceCommitCleanup").bindTo(this));
    }

    @SuppressWarnings("unused")
    public void forceCommitCleanup()
    {
        cleaner.coordinatorRemoveOldCommits();
    }
}
