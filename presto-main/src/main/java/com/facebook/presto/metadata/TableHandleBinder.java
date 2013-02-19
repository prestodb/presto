package com.facebook.presto.metadata;

import com.google.inject.Binder;
import com.google.inject.TypeLiteral;
import com.google.inject.binder.LinkedBindingBuilder;
import com.google.inject.multibindings.MapBinder;

public class TableHandleBinder
{
    public static LinkedBindingBuilder<Class<? extends TableHandle>> bindTableHandle(Binder binder, String tableHandlerType)
    {
        MapBinder<String, Class<? extends TableHandle>> tableHandleTypes = MapBinder.newMapBinder(
                binder,
                new TypeLiteral<String>() {},
                new TypeLiteral<Class<? extends TableHandle>>() {});

        return tableHandleTypes.addBinding(tableHandlerType);
    }
}
