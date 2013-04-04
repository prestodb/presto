package com.facebook.presto.cli;

public final class Help
{
    private Help() {}

    public static String getHelpText()
    {
        return "" +
                "Supported commands:\n" +
                "QUIT\n" +
                "DESCRIBE <table>\n" +
                "SHOW COLUMNS FROM <table>\n" +
                "SHOW FUNCTIONS\n" +
                "SHOW PARTITIONS FROM <table>\n" +
                "SHOW TABLES [LIKE <pattern>]\n" +
                "";
    }
}
