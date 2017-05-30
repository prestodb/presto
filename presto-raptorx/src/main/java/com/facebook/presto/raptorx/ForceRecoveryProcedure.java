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

import com.facebook.presto.raptorx.transaction.TransactionWriter;
import com.facebook.presto.spi.procedure.Procedure;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;
import javax.inject.Provider;

import static com.facebook.presto.raptorx.util.Reflection.getMethod;
import static java.util.Objects.requireNonNull;

public class ForceRecoveryProcedure
        implements Provider<Procedure>
{
    private final TransactionWriter writer;

    @Inject
    public ForceRecoveryProcedure(TransactionWriter writer)
    {
        this.writer = requireNonNull(writer, "writer is null");
    }

    @Override
    public Procedure get()
    {
        return new Procedure(
                "system",
                "force_recovery",
                ImmutableList.of(),
                getMethod(getClass(), "forceRecovery").bindTo(this));
    }

    @SuppressWarnings("unused")
    public void forceRecovery()
    {
        writer.recover();
    }
}
