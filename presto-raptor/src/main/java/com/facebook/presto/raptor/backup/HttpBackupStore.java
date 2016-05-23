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
package com.facebook.presto.raptor.backup;

import com.facebook.presto.spi.PrestoException;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import io.airlift.http.client.BodyGenerator;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.Request;
import io.airlift.http.client.Response;
import io.airlift.http.client.ResponseHandler;
import io.airlift.http.client.StatusResponseHandler.StatusResponse;
import io.airlift.slice.Slices;
import io.airlift.slice.XxHash64;

import javax.inject.Inject;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.UUID;
import java.util.function.Supplier;

import static com.facebook.presto.raptor.RaptorErrorCode.RAPTOR_BACKUP_ERROR;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.MediaType.APPLICATION_BINARY;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.Request.Builder.prepareDelete;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.Request.Builder.prepareHead;
import static io.airlift.http.client.Request.Builder.preparePut;
import static io.airlift.http.client.ResponseHandlerUtils.propagate;
import static io.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class HttpBackupStore
        implements BackupStore
{
    public static final String PRESTO_ENVIRONMENT = "X-Presto-Environment";
    public static final String CONTENT_XXH64 = "X-Content-XXH64";

    private final HttpClient httpClient;
    private final Supplier<URI> baseUriSupplier;
    private final String environment;

    @Inject
    public HttpBackupStore(
            @ForHttpBackup HttpClient httpClient,
            @ForHttpBackup Supplier<URI> baseUriSupplier,
            @ForHttpBackup String environment)
    {
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.baseUriSupplier = requireNonNull(baseUriSupplier, "baseUriSupplier is null");
        this.environment = requireNonNull(environment, "environment is null");
    }

    @Override
    public void backupShard(UUID uuid, File source)
    {
        Request request = preparePut()
                .addHeader(PRESTO_ENVIRONMENT, environment)
                .addHeader(CONTENT_TYPE, APPLICATION_BINARY.toString())
                .addHeader(CONTENT_XXH64, format("%016x", xxHash64(source)))
                .setUri(shardUri(uuid))
                .setBodyGenerator(new FileBodyGenerator(source))
                .build();

        try {
            StatusResponse status = httpClient.execute(request, createStatusResponseHandler());
            if (!isOk(status)) {
                throw badResponse(status);
            }
        }
        catch (RuntimeException e) {
            throw new PrestoException(RAPTOR_BACKUP_ERROR, "Failed to backup shard: " + uuid, e);
        }
    }

    @Override
    public void restoreShard(UUID uuid, File target)
    {
        Request request = prepareGet()
                .addHeader(PRESTO_ENVIRONMENT, environment)
                .setUri(shardUri(uuid))
                .build();

        try {
            StatusResponse status = httpClient.execute(request, new FileResponseHandler(target));
            if (isNotFound(status) || isGone(status)) {
                throw new PrestoException(RAPTOR_BACKUP_ERROR, "Backup shard not found: " + uuid);
            }
            if (!isOk(status)) {
                throw badResponse(status);
            }
        }
        catch (IOException | RuntimeException e) {
            throw new PrestoException(RAPTOR_BACKUP_ERROR, "Failed to restore shard: " + uuid, e);
        }
    }

    @Override
    public boolean deleteShard(UUID uuid)
    {
        Request request = prepareDelete()
                .addHeader(PRESTO_ENVIRONMENT, environment)
                .setUri(shardUri(uuid))
                .build();

        try {
            StatusResponse status = httpClient.execute(request, createStatusResponseHandler());
            if (isOk(status) || isGone(status)) {
                return true;
            }
            if (isNotFound(status)) {
                return false;
            }
            throw badResponse(status);
        }
        catch (RuntimeException e) {
            throw new PrestoException(RAPTOR_BACKUP_ERROR, "Failed to delete shard: " + uuid, e);
        }
    }

    @Override
    public boolean shardExists(UUID uuid)
    {
        Request request = prepareHead()
                .addHeader(PRESTO_ENVIRONMENT, environment)
                .setUri(shardUri(uuid))
                .build();

        try {
            StatusResponse status = httpClient.execute(request, createStatusResponseHandler());
            if (isOk(status)) {
                return true;
            }
            if (isNotFound(status) || isGone(status)) {
                return false;
            }
            throw badResponse(status);
        }
        catch (RuntimeException e) {
            throw new PrestoException(RAPTOR_BACKUP_ERROR, "Failed to check if shard exists: " + uuid, e);
        }
    }

    private URI shardUri(UUID uuid)
    {
        return uriBuilderFrom(baseUriSupplier.get())
                .appendPath(uuid.toString().toLowerCase(ENGLISH))
                .build();
    }

    private static boolean isOk(StatusResponse response)
    {
        return (response.getStatusCode() == HttpStatus.OK.code()) ||
                (response.getStatusCode() == HttpStatus.NO_CONTENT.code());
    }

    private static boolean isNotFound(StatusResponse response)
    {
        return response.getStatusCode() == HttpStatus.NOT_FOUND.code();
    }

    private static boolean isGone(StatusResponse response)
    {
        return response.getStatusCode() == HttpStatus.GONE.code();
    }

    private static RuntimeException badResponse(StatusResponse response)
    {
        throw new RuntimeException("Request failed with HTTP status " + response.getStatusCode());
    }

    private static long xxHash64(File file)
    {
        try {
            return XxHash64.hash(Slices.mapFileReadOnly(file));
        }
        catch (IOException e) {
            throw new PrestoException(RAPTOR_BACKUP_ERROR, "Failed to read file: " + file, e);
        }
    }

    // TODO: move to Airlift
    private static class FileBodyGenerator
            implements BodyGenerator
    {
        private final File file;

        private FileBodyGenerator(File file)
        {
            this.file = requireNonNull(file, "file is null");
        }

        @Override
        public void write(OutputStream out)
                throws Exception
        {
            Files.copy(file, out);
        }
    }

    private static class FileResponseHandler
            implements ResponseHandler<StatusResponse, IOException>
    {
        private final File file;

        private FileResponseHandler(File file)
        {
            this.file = requireNonNull(file, "file is null");
        }

        @Override
        public StatusResponse handleException(Request request, Exception exception)
                throws IOException
        {
            throw propagate(request, exception);
        }

        @Override
        public StatusResponse handle(Request request, Response response)
                throws IOException
        {
            StatusResponse status = createStatusResponse(response);
            if (isOk(status)) {
                writeFile(response.getInputStream());
            }
            return status;
        }

        private void writeFile(InputStream in)
                throws IOException
        {
            try (FileOutputStream out = new FileOutputStream(file)) {
                ByteStreams.copy(in, out);
                out.flush();
                out.getFD().sync();
            }
        }

        private static StatusResponse createStatusResponse(Response response)
        {
            return new StatusResponse(response.getStatusCode(), response.getStatusMessage(), response.getHeaders());
        }
    }
}
