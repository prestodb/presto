package com.facebook.presto.cli;

import com.facebook.presto.client.ClientSession;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import io.airlift.http.client.netty.StandaloneNettyAsyncHttpClient;
import jline.console.completer.Completer;

import java.io.Closeable;
import java.util.Collection;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import static com.google.common.base.Preconditions.checkNotNull;

public class TableNameCompleter
        implements Completer, Closeable
{
    private final ClientSession clientSession;
    private final HttpMetadataClient metadataClient;

    public TableNameCompleter(ClientSession clientSession)
    {
        this.clientSession = checkNotNull(clientSession, "client session was null!");
        this.metadataClient = new HttpMetadataClient(clientSession, new StandaloneNettyAsyncHttpClient("metadata"));
    }

    @Override
    public int complete(String buffer, int cursor, List<CharSequence> candidates)
    {
        if (cursor <= 0) {
            return cursor;
        }
        int blankPos = findLastBlank(buffer.substring(0, cursor));

        String prefix = buffer.substring(blankPos + 1, cursor);
        List<String> completion = ImmutableList.copyOf(Splitter.on('.').limit(3).split(prefix));

        int len = 0;
        SortedSet<String> sortedCandidates = new TreeSet<>();
        switch (completion.size()) {
        case 0:
            break;
        case 1:
            loadTableNames(sortedCandidates, clientSession.getCatalog(), clientSession.getSchema(), completion.get(0));
            loadSchemaNames(sortedCandidates, clientSession.getCatalog(), completion.get(0));
            break;
        case 2:
            // only complete on the last field of the dot separated name.
            len += completion.get(0).length() + 1;
            loadSchemaNames(sortedCandidates, completion.get(0), completion.get(1));
            loadTableNames(sortedCandidates, clientSession.getCatalog(), completion.get(0), completion.get(1));
            break;
        default:
            // only complete on the last field of the dot separated name.
            len += completion.get(0).length() + 1;
            len += completion.get(1).length() + 1;
            loadTableNames(sortedCandidates, completion.get(0), completion.get(1), completion.get(2));
            break;
        }
        candidates.addAll(sortedCandidates);
        return blankPos + len + 1;
    }

    private int findLastBlank(String buffer)
    {
        for (int i = buffer.length() - 1; i >= 0 ; i--) {
            if (Character.isWhitespace(buffer.charAt(i))) {
                return i;
            }
        }
        return -1;
    }

    private void loadSchemaNames(Collection<String> candidates, String catalogName, String prefix)
    {
        if (!Strings.isNullOrEmpty(catalogName)) {
            List<String> schemaNames = metadataClient.getSchemaNames(catalogName);
            List<String> results = filterResults(schemaNames, prefix);
            candidates.addAll(results);
        }
    }

    private void loadTableNames(Collection<String> candidates, String catalogName, String schemaName, String prefix)
    {
        if (!Strings.isNullOrEmpty(catalogName) && !Strings.isNullOrEmpty(schemaName)) {
            List<String> tableNames = metadataClient.getTableNames(catalogName, schemaName);
            List<String> results = filterResults(tableNames, prefix);
            candidates.addAll(results);
        }
    }

    private static List<String> filterResults(List<String> values, String prefix)
    {
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        for (String value : values) {
            if (value.startsWith(prefix)) {
                builder.add(value);
            }
        }
        return builder.build();
    }

    @Override
    public void close()
    {
        metadataClient.close();
    }
}
