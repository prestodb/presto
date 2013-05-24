package com.facebook.presto.operator.scalar;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.awt.Color;

import static com.facebook.presto.operator.scalar.StringFunctions.upper;
import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Preconditions.checkArgument;

public class ColorFunctions
{
    private enum SystemColor
    {
        BLACK(0),
        RED(1),
        GREEN(2),
        YELLOW(3),
        BLUE(4),
        MAGENTA(5),
        CYAN(6),
        WHITE(7);

        private final int index;

        SystemColor(int index)
        {
            this.index = index;
        }

        private int getIndex()
        {
            return index;
        }
    }

    @ScalarFunction
    public static long color(Slice color)
    {
        checkArgument(color.length() > 0, "Invalid color: ''");

        int rgb = parseRgb(color);

        if (rgb != -1) {
            return rgb;
        }

        // encode system colors (0-15) as negative values, offset by one
        int index = SystemColor.valueOf(upper(color).toString(UTF_8)).getIndex();
        return -(index + 1);
    }

    @ScalarFunction
    public static long rgb(long red, long green, long blue)
    {
        checkArgument(red >= 0 && red <= 255, "red must be between 0 and 255");
        checkArgument(green >= 0 && green <= 255, "green must be between 0 and 255");
        checkArgument(blue >= 0 && blue <= 255, "blue must be between 0 and 255");

        return (red << 16) | (green << 8) | blue;
    }

    /**
     * Interpolate a color between lowColor and highColor based the provided value
     *
     * The value is truncated to the range [low, high] if it's outside.
     * Color can be any of the system colors (red, green, blue, etc.) or an rgb value of the form #rgb
     */
    @ScalarFunction
    public static long color(double value, double low, double high, Slice lowColor, Slice highColor)
    {
        return color((value - low) * 1.0 / (high - low), lowColor, highColor);
    }

    /**
     * Interpolate a color between lowColor and highColor based on the provided value
     *
     * The value is truncated to the range [0, 1] if necessary
     * Color can be any of the system colors (red, green, blue, etc.) or an rgb value of the form #rgb
     */
    @ScalarFunction
    public static long color(double fraction, Slice lowColor, Slice highColor)
    {
        int lowRgb = parseRgb(lowColor);
        int highRgb = parseRgb(highColor);

        Preconditions.checkArgument(lowRgb != -1, "lowColor not a valid RGB color: %s", lowColor.toString(Charsets.UTF_8));
        Preconditions.checkArgument(highRgb != -1, "highColor not a valid RGB color: %s", lowColor.toString(Charsets.UTF_8));

        fraction = Math.min(1, fraction);
        fraction = Math.max(0, fraction);

        return interpolate((float) fraction, lowRgb, highRgb);
    }

    @ScalarFunction
    public static Slice render(Slice value, long color)
    {
        StringBuilder builder = new StringBuilder(value.length());

        // color
        builder.append("\u001b[38;5;")
                .append(toAnsi(color))
                .append('m');

        // value
        builder.append(value.toString(Charsets.UTF_8));

        // reset
        builder.append("\u001b[0m");

        return Slices.copiedBuffer(builder.toString(), Charsets.UTF_8);
    }

    @ScalarFunction
    public static Slice render(long value, long color)
    {
        return render(Slices.copiedBuffer(Long.toString(value), Charsets.UTF_8), color);
    }

    @ScalarFunction
    public static Slice render(double value, long color)
    {
        return render(Slices.copiedBuffer(Double.toString(value), Charsets.UTF_8), color);
    }


    private static int interpolate(float fraction, long lowRgb, long highRgb)
    {
        float[] lowHsv = Color.RGBtoHSB(getRed(lowRgb), getGreen(lowRgb), getBlue(lowRgb), null);
        float[] highHsv = Color.RGBtoHSB(getRed(highRgb), getGreen(highRgb), getBlue(highRgb), null);

        float h = fraction * (highHsv[0] - lowHsv[0]) + lowHsv[0];
        float s = fraction * (highHsv[1] - lowHsv[1]) + lowHsv[1];
        float v = fraction * (highHsv[2] - lowHsv[2]) + lowHsv[2];

        return Color.HSBtoRGB(h, s, v) & 0xFF_FF_FF;
    }

    /**
     * Convert the given color (rgb or system) to an ansi-compatible index (for use with ESC[38;5;<value>m)
     */
    private static int toAnsi(int red, int green, int blue)
    {
        // rescale to 0-5 range
        red = red * 6 / 256;
        green = green * 6 / 256;
        blue = blue * 6 / 256;

        return 16 + red * 36 + green * 6 + blue;
    }

    /**
     * Convert the given color (rgb or system) to an ansi-compatible index (for use with ESC[38;5;<value>m)
     */
    private static int toAnsi(long color)
    {
        if (color >= 0) { // an rgb value encoded as in Color.getRGB
            return toAnsi(getRed(color), getGreen(color), getBlue(color));
        }
        else {
            return (int) (-color - 1);
        }
    }

    @VisibleForTesting
    static int parseRgb(Slice color)
    {
        if (color.length() != 4 || color.getByte(0) != '#') {
            return -1;
        }

        int red = Character.digit((char) color.getByte(1), 16);
        int green = Character.digit((char) color.getByte(2), 16);
        int blue = Character.digit((char) color.getByte(3), 16);

        if (red == -1 || green == -1 || blue == -1) {
            return -1;
        }

        // replicate the nibbles to turn a color of the form #rgb => #rrggbb (css semantics)
        red = (red << 4) | red;
        green = (green << 4) | green;
        blue = (blue << 4) | blue;

        return (int) rgb(red, green, blue);
    }

    @VisibleForTesting
    static int getRed(long color)
    {
        checkArgument(color >= 0, "color is not a valid rgb value");

        return (int) ((color >>> 16) & 0xff);
    }

    @VisibleForTesting
    static int getGreen(long color)
    {
        checkArgument(color >= 0, "color is not a valid rgb value");

        return (int) ((color >>> 8) & 0xff);
    }

    @VisibleForTesting
    static int getBlue(long color)
    {
        checkArgument(color >= 0, "color is not a valid rgb value");

        return (int) (color & 0xff);
    }
}
