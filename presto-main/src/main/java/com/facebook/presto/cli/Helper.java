package com.facebook.presto.cli;

import java.util.Random;

/**
 * Basic HELP and HINT system.
 *
 * @TODO The current zorkmid implementation should be replaced with a real HELP/HINT system.
 */
final class Helper
{
    static final String [] HELP = {
        "\u0054\u0068\u0065\u0072\u0065\u0020\u0069\u0073\u0020\u006e\u006f\u0020" +
        "\u006f\u006e\u0065\u0020\u0068\u0065\u0072\u0065\u0020\u0074\u006f\u0020" +
        "\u0068\u0065\u006c\u0070\u0020\u0079\u006f\u0075\u002e",
        "\u0059\u006f\u0075\u0020\u0073\u0068\u006f\u0075\u0074\u0020\"" +
        "\u0048\u0045\u004c\u0050\u002c\u0020\u0048\u0045\u004c\u0050\u0021" +
        "\"\u002e\u0020\u004e\u006f\u0020\u006f\u006e\u0065\u0020\u0072\u0065" +
        "\u0070\u006c\u0069\u0065\u0073\u002e",
        "\u0054\u0068\u0065\u0072\u0065\u0020\u006d\u0069\u0067\u0068\u0074\u0020" +
        "\u0062\u0065\u0020\u0061\u0020\u0067\u0072\u0075\u0065\u0020\u006e\u0065" +
        "\u0061\u0072\u0062\u0079\u002e\u0020\u0043\u0061\u006c\u006c\u0069\u006e" +
        "\u0067\u0020\u0066\u006f\u0072\u0020\u0068\u0065\u006c\u0070\u0020\u0063" +
        "\u006f\u0075\u006c\u0064\u0020\u0061\u0074\u0074\u0072\u0061\u0063\u0074" +
        "\u0020\u0069\u0074\u002e"
    };

    static final String [] HINT = {
        "\u0043\u0061\u006c\u006c\u0020\u0031\u002d\u0039\u0030\u0030\u002d\u0047" +
        "\u0045\u0054\u002d\u0048\u0049\u004e\u0054\u0053\u0020\u0074\u006f\u0020" +
        "\u0072\u0065\u0063\u0065\u0069\u0076\u0065\u0020\u0068\u0069\u006e\u0074" +
        "\u0073\u002e",
        "\u0048\u0061\u0076\u0065\u0020\u0079\u006f\u0075\u0020\u0063\u0068\u0065" +
        "\u0063\u006b\u0065\u0064\u0020\u0074\u0068\u0065\u0020\u0068\u0069\u006e" +
        "\u0074\u0020\u0062\u006f\u006f\u006b\u003f"
    };

    private static final Random RANDOM = new Random();

    private Helper()
    {
    }

    static String help()
    {
        final int msg = RANDOM.nextInt(HELP.length);
        return HELP[msg];
    }

    static String hint()
    {
        final int msg = RANDOM.nextInt(HINT.length);
        return HINT[msg];
    }
}
