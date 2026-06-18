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
package com.facebook.presto.spi.function;

import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.spi.api.Experimental;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.net.URI;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

@Experimental
public class RestFunctionHandle
        extends SqlFunctionHandle
{
    private final Signature signature;
    private final Optional<URI> executionEndpoint;

    @JsonCreator
    public RestFunctionHandle(
            @JsonProperty("functionId") SqlFunctionId functionId,
            @JsonProperty("version") String version,
            @JsonProperty("signature") Signature signature,
            @JsonProperty("executionEndpoint") Optional<URI> executionEndpoint)
    {
        super(functionId, version);
        this.signature = requireNonNull(signature, "signature is null");
        this.executionEndpoint = requireNonNull(executionEndpoint, "executionEndpoint is null");
        executionEndpoint.ifPresent(uri -> {
            String scheme = uri.getScheme();
            if (!"http".equals(scheme) && !"https".equals(scheme)) {
                throw new IllegalArgumentException("Execution endpoint must use HTTP or HTTPS protocol: " + uri);
            }
        });
    }

    public RestFunctionHandle(
            SqlFunctionId functionId,
            String version,
            Signature signature)
    {
        this(functionId, version, signature, Optional.empty());
    }

    @JsonProperty
    public Signature getSignature()
    {
        return signature;
    }

    @JsonProperty
    public Optional<URI> getExecutionEndpoint()
    {
        return executionEndpoint;
    }

    @Override
    @JsonIgnore
    public List<TypeSignature> getArgumentTypes()
    {
        return super.getArgumentTypes();
    }

    public static class Resolver
            implements FunctionHandleResolver
    {
        @Override
        public Class<? extends FunctionHandle> getFunctionHandleClass()
        {
            return RestFunctionHandle.class;
        }
    }
}
