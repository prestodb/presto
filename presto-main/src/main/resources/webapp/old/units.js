function formatDuration(millis) {
    var unit = "ms";
    if (millis >= 1000) {
        millis /= 1000;
        unit = "s";
    }
    if (millis >= 60) {
        millis /= 60;
        unit = "m";
    }
    if (millis >= 60) {
        millis /= 60;
        unit = "h";
    }

    return precisionRound(millis) + " " + unit;
}

function formatDataSize(bytes) {
    var unit = "B";
    if (bytes >= 1024) {
        bytes /= 1024;
        unit = "KB";
    }
    if (bytes >= 1024) {
        bytes /= 1024;
        unit = "MB";
    }
    if (bytes >= 1024) {
        bytes /= 1024;
        unit = "GB";
    }
    if (bytes >= 1024) {
        bytes /= 1024;
        unit = "TB";
    }
    if (bytes >= 1024) {
        bytes /= 1024;
        unit = "PB";
    }
    return precisionRound(bytes) + " " + unit;
}

function formatCount(count) {
    var unit = "";
    if (count > 1000) {
        count /= 1000;
        unit = "K";
    }
    if (count > 1000) {
        count /= 1000;
        unit = "M";
    }
    if (count > 1000) {
        count /= 1000;
        unit = "B";
    }
    if (count > 1000) {
        count /= 1000;
        unit = "T";
    }
    if (count > 1000) {
        count /= 1000;
        unit = "Q";
    }
    return precisionRound(count) + unit;
}

function precisionRound(n) {
    if (n < 10) {
        return d3.round(n, 2);
    }
    if (n < 100) {
        return d3.round(n, 1);
    }
    return Math.round(n);
}
