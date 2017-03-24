package com.facebook.presto.baseplugin;

import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.type.Type;

import java.util.List;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * Created by amehta on 6/13/16.
 */
public class BaseRecordSet implements RecordSet {
    private final BaseQuery baseQuery;
    private final BaseProvider baseProvider;

    public BaseRecordSet(BaseQuery baseQuery, BaseProvider baseProvider) {
        this.baseQuery = requireNonNull(baseQuery, "baseQuery is null");
        this.baseProvider = requireNonNull(baseProvider, "baseProvider is null");
    }

    public BaseQuery getBaseQuery(){
        return baseQuery;
    }

    public BaseProvider getBaseProvider() {
        return baseProvider;
    }

    @Override
    public List<Type> getColumnTypes() {
        return baseQuery.getDesiredColumns().stream().map(x -> ((BaseColumnHandle)x).getColumnType()).collect(Collectors.toList());
    }

    @Override
    public RecordCursor cursor() {
        return new BaseRecordCursor(baseQuery, baseProvider);
    }
}
