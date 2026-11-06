import datetime
import struct
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq

filename = "min_max_stats/min_max_fixture.parquet"

# Every column has 100 rows and 0 nulls. The writer (pyarrow) populates min_value/max_value for all
# columns, and also the deprecated min/max for columns that have a signed sort order. In step 2 we
# patch the footer so each column has exactly one of the two sets of fields:
#   - MODERN_ONLY columns : only min_value/max_value are set
#   - LEGACY_ONLY columns : only the deprecated min/max are set
#
# Readers should handle the columns as follows (see parquet-java's ParquetMetadataConverter) :
#   - MODERN_ONLY : stats are always read
#   - LEGACY_ONLY : stats are only read if the column has a signed sort order (signed ints, date, time,
#                   timestamp, float, double) or if min == max. Unsigned ints and decimals are ignored.
ROWS = range(1, 101)
base_date = datetime.date(2022, 1, 9)
base_ts = datetime.datetime(2022, 1, 9, 10, 30, 0)

columns = {
    # Original columns, kept first and unchanged. Physical types only; no logical annotation except date
    "i32": (pa.int32(), [i for i in ROWS], "modern"),
    "i64": (pa.int64(), [i * 1_000_000_000 for i in ROWS], "modern"),
    "d": (pa.date32(), [base_date + datetime.timedelta(days=i) for i in range(100)], "modern"),
    "dbl": (pa.float64(), [i + 0.5 for i in ROWS], "modern"),

    # More types with only min_value/max_value
    "flt": (pa.float32(), [i + 0.25 for i in ROWS], "modern"),
    "i8": (pa.int8(), [i for i in ROWS], "modern"),
    "i16": (pa.int16(), [i * 100 for i in ROWS], "modern"),
    "ts": (pa.timestamp("us"), [base_ts + datetime.timedelta(minutes=i) for i in range(100)], "modern"),
    "dec32": (pa.decimal128(9, 2), [Decimal(i) / 100 for i in ROWS], "modern"),
    "dec64": (pa.decimal128(18, 3), [Decimal(i * 1_000_000) / 1000 for i in ROWS], "modern"),
    # Unsigned columns : the sort order is unsigned, but min_value/max_value are always trusted.
    "u8": (pa.uint8(), [i for i in ROWS], "modern"),
    "u32": (pa.uint32(), [i * 10_000_000 for i in ROWS], "modern"),
    "u64": (pa.uint64(), [i * 1_000_000_000 for i in ROWS], "modern"),

    # Columns with only the deprecated min/max, and a signed sort order : these are read
    "i32_legacy": (pa.int32(), [i for i in ROWS], "legacy"),
    "i64_legacy": (pa.int64(), [i * 1_000_000_000 for i in ROWS], "legacy"),
    "d_legacy": (pa.date32(), [base_date + datetime.timedelta(days=i) for i in range(100)], "legacy"),
    "dbl_legacy": (pa.float64(), [i + 0.5 for i in ROWS], "legacy"),
    "flt_legacy": (pa.float32(), [i + 0.25 for i in ROWS], "legacy"),
    "i8_legacy": (pa.int8(), [i for i in ROWS], "legacy"),
    "i16_legacy": (pa.int16(), [i * 100 for i in ROWS], "legacy"),
    "ts_legacy": (pa.timestamp("us"), [base_ts + datetime.timedelta(minutes=i) for i in range(100)], "legacy"),

    # Columns with only the deprecated min/max, and an unsigned or unknown sort order : these are ignored...
    "u8_legacy": (pa.uint8(), [i for i in ROWS], "legacy"),
    "u32_legacy": (pa.uint32(), [i * 10_000_000 for i in ROWS], "legacy"),
    "u64_legacy": (pa.uint64(), [i * 1_000_000_000 for i in ROWS], "legacy"),
    "dec32_legacy": (pa.decimal128(9, 2), [Decimal(i) / 100 for i in ROWS], "legacy"),
    "dec64_legacy": (pa.decimal128(18, 3), [Decimal(i * 1_000_000) / 1000 for i in ROWS], "legacy"),
    # ...unless min == max, in which case the sort order does not matter
    "u32_const_legacy": (pa.uint32(), [7] * 100, "legacy"),
}

legacy_only = {name for name, (_, _, mode) in columns.items() if mode == "legacy"}

# 1. Build 100 rows matching exact bounds and 0 nulls
table = pa.table({name: pa.array(values, type=typ) for name, (typ, values, _) in columns.items()})

# store_decimal_as_integer writes DECIMAL(9, 2) as INT32 and DECIMAL(18, 3) as INT64, so that the footer stats
# are visible to the quick stats builder
pq.write_table(table, filename, write_statistics=True, store_decimal_as_integer=True)

# 2. Read and patch the footer
#
# The footer is a thrift compact protocol encoded FileMetaData. We use a minimal generic thrift codec (below) instead
# of a parquet library, since the libraries re-serialize the footer through their own model, which is lossy or buggy for
# some of the logical types that we want in this fixture, and they make it hard to unset only some of the fields.
STOP, BOOL_TRUE, BOOL_FALSE, BYTE, I16, I32, I64, DOUBLE, BINARY, LIST, SET, MAP, STRUCT = range(13)


class Reader:
    def __init__(self, buf):
        self.buf, self.pos = buf, 0

    def byte(self):
        self.pos += 1
        return self.buf[self.pos - 1]

    def varint(self):
        result, shift = 0, 0
        while True:
            b = self.byte()
            result |= (b & 0x7F) << shift
            if not b & 0x80:
                return result
            shift += 7

    def zigzag(self):
        n = self.varint()
        return (n >> 1) ^ -(n & 1)

    def value(self, ttype):
        if ttype in (BOOL_TRUE, BOOL_FALSE):
            return self.byte() == BOOL_TRUE  # bool list element
        if ttype == BYTE:
            return self.byte()
        if ttype in (I16, I32, I64):
            return self.zigzag()
        if ttype == DOUBLE:
            self.pos += 8
            return struct.unpack("<d", self.buf[self.pos - 8:self.pos])[0]
        if ttype == BINARY:
            n = self.varint()
            self.pos += n
            return self.buf[self.pos - n:self.pos]
        if ttype in (LIST, SET):
            header = self.byte()
            size, etype = header >> 4, header & 0x0F
            if size == 15:
                size = self.varint()
            return (etype, [self.value(etype) for _ in range(size)])
        if ttype == MAP:
            size = self.varint()
            if size == 0:
                return (0, 0, [])
            kv = self.byte()
            ktype, vtype = kv >> 4, kv & 0x0F
            return (ktype, vtype, [(self.value(ktype), self.value(vtype)) for _ in range(size)])
        if ttype == STRUCT:
            return self.struct()
        raise ValueError(f"unknown thrift type {ttype}")

    def struct(self):
        """A struct is a dict of field id -> (type, value). Dicts preserve the insertion (wire) order."""
        fields, last_id = {}, 0
        while True:
            header = self.byte()
            if header == STOP:
                return fields
            ttype, delta = header & 0x0F, header >> 4
            fid = last_id + delta if delta else self.zigzag()
            if ttype in (BOOL_TRUE, BOOL_FALSE):
                fields[fid] = (ttype, ttype == BOOL_TRUE)
            else:
                fields[fid] = (ttype, self.value(ttype))
            last_id = fid


class Writer:
    def __init__(self):
        self.out = bytearray()

    def varint(self, n):
        while n > 0x7F:
            self.out.append((n & 0x7F) | 0x80)
            n >>= 7
        self.out.append(n)

    def zigzag(self, n):
        self.varint((n << 1) ^ (n >> 63))

    def value(self, ttype, v):
        if ttype in (BOOL_TRUE, BOOL_FALSE):
            self.out.append(BOOL_TRUE if v else BOOL_FALSE)
        elif ttype == BYTE:
            self.out.append(v)
        elif ttype in (I16, I32, I64):
            self.zigzag(v)
        elif ttype == DOUBLE:
            self.out += struct.pack("<d", v)
        elif ttype == BINARY:
            self.varint(len(v))
            self.out += v
        elif ttype in (LIST, SET):
            etype, elems = v
            if len(elems) < 15:
                self.out.append((len(elems) << 4) | etype)
            else:
                self.out.append(0xF0 | etype)
                self.varint(len(elems))
            for e in elems:
                self.value(etype, e)
        elif ttype == MAP:
            ktype, vtype, entries = v
            self.varint(len(entries))
            if entries:
                self.out.append((ktype << 4) | vtype)
                for k, val in entries:
                    self.value(ktype, k)
                    self.value(vtype, val)
        elif ttype == STRUCT:
            self.struct(v)
        else:
            raise ValueError(f"unknown thrift type {ttype}")

    def struct(self, fields):
        last_id = 0
        for fid in sorted(fields):
            ttype, v = fields[fid]
            if ttype in (BOOL_TRUE, BOOL_FALSE):
                ttype = BOOL_TRUE if v else BOOL_FALSE
            delta = fid - last_id
            if 0 < delta <= 15:
                self.out.append((delta << 4) | ttype)
            else:
                self.out.append(ttype)
                self.zigzag(fid)
            if ttype not in (BOOL_TRUE, BOOL_FALSE):
                self.value(ttype, v)
            last_id = fid
        self.out.append(STOP)


# Field ids from parquet.thrift
FILE_META_DATA_ROW_GROUPS = 4
ROW_GROUP_COLUMNS = 1
COLUMN_CHUNK_META_DATA = 3
COLUMN_META_DATA_PATH_IN_SCHEMA = 3
COLUMN_META_DATA_STATISTICS = 12
STATS_MAX, STATS_MIN, STATS_NULL_COUNT, STATS_MAX_VALUE, STATS_MIN_VALUE = 1, 2, 3, 5, 6


def column_chunks(fmd):
    for row_group in fmd[FILE_META_DATA_ROW_GROUPS][1][1]:
        for column_chunk in row_group[ROW_GROUP_COLUMNS][1][1]:
            meta_data = column_chunk[COLUMN_CHUNK_META_DATA][1]
            name = meta_data[COLUMN_META_DATA_PATH_IN_SCHEMA][1][1][0].decode()
            yield name, meta_data


with open(filename, "rb") as f:
    data = f.read()

footer_len = struct.unpack("<i", data[-8:-4])[0]
body_and_pages = data[:-8 - footer_len]
fmd = Reader(data[-8 - footer_len:-8]).struct()

for name, meta_data in column_chunks(fmd):
    stats = meta_data[COLUMN_META_DATA_STATISTICS][1]
    # Writers only fill in the deprecated fields for some columns, min_value/max_value is always present
    min_bytes = stats[STATS_MIN_VALUE][1] if STATS_MIN_VALUE in stats else stats[STATS_MIN][1]
    max_bytes = stats[STATS_MAX_VALUE][1] if STATS_MAX_VALUE in stats else stats[STATS_MAX][1]
    for fid in (STATS_MIN, STATS_MAX, STATS_MIN_VALUE, STATS_MAX_VALUE):
        stats.pop(fid, None)
    if name in legacy_only:
        stats[STATS_MIN], stats[STATS_MAX] = (BINARY, min_bytes), (BINARY, max_bytes)
    else:
        stats[STATS_MIN_VALUE], stats[STATS_MAX_VALUE] = (BINARY, min_bytes), (BINARY, max_bytes)
    stats[STATS_NULL_COUNT] = (I64, 0)

writer = Writer()
writer.struct(fmd)
new_footer = bytes(writer.out)
with open(filename, "wb") as f:
    f.write(body_and_pages)
    f.write(new_footer)
    f.write(struct.pack("<i", len(new_footer)))
    f.write(b"PAR1")

# 3. Verify, by re-reading the footer from the file
with open(filename, "rb") as f:
    data = f.read()
footer_len = struct.unpack("<i", data[-8:-4])[0]
fmd = Reader(data[-8 - footer_len:-8]).struct()
pq.read_metadata(filename)  # the file must still be readable by a regular parquet reader

print(f"{'Column':<17} | {'num_values':<10} | {'null_count':<10} | {'min/max':<8} | {'min_value/max_value'}")
print("-" * 80)
for name, meta_data in column_chunks(fmd):
    s = meta_data[COLUMN_META_DATA_STATISTICS][1]
    legacy = "set" if (STATS_MIN in s and STATS_MAX in s) else "unset"
    modern = "set" if (STATS_MIN_VALUE in s and STATS_MAX_VALUE in s) else "unset"
    expected = ("set", "unset") if name in legacy_only else ("unset", "set")
    assert (legacy, modern) == expected, f"{name}: unexpected stats fields {(legacy, modern)}, expected {expected}"
    print(f"{name:<17} | {meta_data[5][1]:<10} | {s[STATS_NULL_COUNT][1]:<10} | {legacy:<8} | {modern}")
