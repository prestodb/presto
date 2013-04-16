package com.facebook.presto.metadata;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.TableHandle;
import com.google.inject.Binder;
import com.google.inject.TypeLiteral;
import com.google.inject.binder.LinkedBindingBuilder;
import com.google.inject.multibindings.MapBinder;

public class HandleJsonBinder
{
    public static LinkedBindingBuilder<Class<? extends TableHandle>> bindTableHandle(Binder binder, String tableHandlerType)
    {
        MapBinder<String, Class<? extends TableHandle>> tableHandleTypes = MapBinder.newMapBinder(
                binder,
                new TypeLiteral<String>() {},
                new TypeLiteral<Class<? extends TableHandle>>() {});

        return tableHandleTypes.addBinding(tableHandlerType);
    }

    public static LinkedBindingBuilder<Class<? extends ColumnHandle>> bindColumnHandle(Binder binder, String columnHandlerType)
    {
        MapBinder<String, Class<? extends ColumnHandle>> columnHandleTypes = MapBinder.newMapBinder(
                binder,
                new TypeLiteral<String>() {},
                new TypeLiteral<Class<? extends ColumnHandle>>() {});

        return columnHandleTypes.addBinding(columnHandlerType);
    }
}
