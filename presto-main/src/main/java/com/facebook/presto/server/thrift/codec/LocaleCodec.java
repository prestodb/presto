package com.facebook.presto.server.thrift.codec;

import com.facebook.drift.codec.ThriftCodec;
import com.facebook.drift.codec.internal.coercion.FromThrift;
import com.facebook.drift.codec.internal.coercion.ToThrift;
import com.facebook.drift.codec.metadata.ThriftCatalog;
import com.facebook.drift.codec.metadata.ThriftType;
import com.facebook.drift.protocol.TProtocolReader;
import com.facebook.drift.protocol.TProtocolWriter;

import javax.inject.Inject;

import java.util.Locale;

import static java.util.Objects.requireNonNull;

public class LocaleCodec
        implements ThriftCodec<Locale>
{
    @Inject
    public LocaleCodec(ThriftCatalog thriftCatalog)
    {
        thriftCatalog.addDefaultCoercions(getClass());
    }

    @Override
    public ThriftType getType()
    {
        return new ThriftType(ThriftType.STRING, Locale.class);
    }

    @Override
    public Locale read(TProtocolReader protocol)
            throws Exception
    {
        return stringToLocale(protocol.readString());
    }

    @Override
    public void write(Locale locale, TProtocolWriter protocol)
            throws Exception
    {
        protocol.writeString(localeToString(locale));
    }

    @FromThrift
    public static Locale stringToLocale(String locale)
    {
        requireNonNull(locale, "locale is null");
        return Locale.forLanguageTag(locale);
    }

    @ToThrift
    public static String localeToString(Locale locale)
    {
        requireNonNull(locale, "locale is null");
        return locale.toLanguageTag();
    }
}

