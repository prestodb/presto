/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.ducklake.split.pruning;

import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.Range;
import com.facebook.presto.common.predicate.SortedRangeSet;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.predicate.ValueSet;
import com.facebook.presto.common.type.DateType;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.ducklake.DuckLakeColumnHandle;
import com.facebook.presto.ducklake.catalog.DuckLakeDataFile;
import com.facebook.presto.ducklake.catalog.DuckLakePartitionField;
import com.facebook.presto.spi.ColumnHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slices;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.groupingBy;

/**
 * Drops {@link DuckLakeDataFile}s whose stored partition values cannot satisfy a predicate,
 * without ever opening the file (spec &sect;4.5). Pruning is exact for {@code identity} and for
 * {@code year} alone; {@code month}/{@code day}/{@code hour} wrap (DuckDB's {@code month(x)} is
 * 1-12, not an ordinal), so on their own they can only be checked for membership in the set of
 * values the predicate's range touches. When two or more transforms of the same source column
 * are declared together (for example {@code month(ts)} and {@code year(ts)}, DuckLake's usual way
 * of partitioning a timestamp by calendar month), this class checks them as one joint tuple
 * instead of two independent ones, which is what actually lets the {@code month}/{@code year}
 * combination exactly resolve a range that straddles a year boundary (spec &sect;4.5 example:
 * November 2024 through January 2025). Whenever a prunable field's range cannot be evaluated
 * exactly cheaply (an unbounded side, or a span too wide to enumerate), pruning backs off: to a
 * year-interval check when the group has a {@code year} member (since {@code year} alone is
 * monotonic and stays exact no matter how wide or one-sided the range is), or otherwise to
 * keeping the file outright. Both directions err toward reading a file, never toward skipping
 * one.
 */
public final class PartitionPruner
{
    private PartitionPruner() {}

    public static List<DuckLakeDataFile> prune(
            List<DuckLakeDataFile> files,
            List<DuckLakePartitionField> partitionFields,
            Map<Long, DuckLakeColumnHandle> columnsById,
            TupleDomain<ColumnHandle> predicate)
    {
        requireNonNull(files, "files is null");
        requireNonNull(partitionFields, "partitionFields is null");
        requireNonNull(columnsById, "columnsById is null");
        requireNonNull(predicate, "predicate is null");

        if (predicate.isAll() || partitionFields.isEmpty()) {
            return files;
        }
        Optional<Map<ColumnHandle, Domain>> domains = predicate.getDomains();
        if (!domains.isPresent()) {
            return files;
        }

        List<IdentityCheck> identityChecks = new ArrayList<>();
        List<CalendarGroup> calendarGroups = new ArrayList<>();

        Map<Long, List<DuckLakePartitionField>> fieldsByColumn = partitionFields.stream()
                .collect(groupingBy(DuckLakePartitionField::getColumnId));

        for (Map.Entry<Long, List<DuckLakePartitionField>> entry : fieldsByColumn.entrySet()) {
            DuckLakeColumnHandle column = columnsById.get(entry.getKey());
            if (column == null) {
                continue;
            }
            Domain domain = domains.get().get(column);
            if (domain == null || domain.isAll()) {
                continue;
            }

            List<CalendarMember> calendarMembers = new ArrayList<>();
            for (DuckLakePartitionField field : entry.getValue()) {
                PartitionTransform transform = PartitionTransform.parse(field.getTransform());
                if (!transform.isPrunable(column.getColumnIdentity().getDuckLakeType())) {
                    continue;
                }
                if (transform.getKind() == PartitionTransform.Kind.IDENTITY) {
                    identityChecks.add(new IdentityCheck(field.getPartitionKeyIndex(), column, domain));
                }
                else {
                    calendarMembers.add(new CalendarMember(field.getPartitionKeyIndex(), transform));
                }
            }
            if (!calendarMembers.isEmpty()) {
                calendarGroups.add(CalendarGroup.build(calendarMembers, domain, column.getType()));
            }
        }

        if (identityChecks.isEmpty() && calendarGroups.isEmpty()) {
            return files;
        }

        ImmutableList.Builder<DuckLakeDataFile> kept = ImmutableList.builder();
        for (DuckLakeDataFile file : files) {
            if (keep(file, identityChecks, calendarGroups)) {
                kept.add(file);
            }
        }
        return kept.build();
    }

    private static boolean keep(DuckLakeDataFile file, List<IdentityCheck> identityChecks, List<CalendarGroup> calendarGroups)
    {
        for (IdentityCheck check : identityChecks) {
            if (!check.keep(file)) {
                return false;
            }
        }
        for (CalendarGroup group : calendarGroups) {
            if (!group.keep(file)) {
                return false;
            }
        }
        return true;
    }

    private static LocalDateTime toLocalDateTime(Object nativeValue, Type sourceType)
    {
        if (sourceType instanceof DateType) {
            return LocalDate.ofEpochDay((Long) nativeValue).atStartOfDay();
        }
        if (sourceType instanceof TimestampType) {
            return LocalDateTime.ofInstant(Instant.ofEpochMilli((Long) nativeValue), ZoneOffset.UTC);
        }
        throw new IllegalArgumentException("Unsupported source type for a calendar transform: " + sourceType);
    }

    private static LocalDateTime truncateTo(LocalDateTime value, ChronoUnit unit)
    {
        switch (unit) {
            case YEARS:
                return LocalDateTime.of(value.getYear(), 1, 1, 0, 0);
            case MONTHS:
                return LocalDateTime.of(value.getYear(), value.getMonthValue(), 1, 0, 0);
            case DAYS:
                return value.toLocalDate().atStartOfDay();
            case HOURS:
                return value.truncatedTo(ChronoUnit.HOURS);
            default:
                throw new IllegalArgumentException("Unsupported step unit: " + unit);
        }
    }

    private static int capFor(ChronoUnit unit)
    {
        switch (unit) {
            case HOURS:
                return 24;
            case DAYS:
                return 31;
            case MONTHS:
                return 12;
            case YEARS:
                // Year alone is monotonic and never wraps, so this is purely a sanity bound on how
                // many discrete tuples a joint calendar group will enumerate, not a correctness limit.
                return 4000;
            default:
                throw new IllegalArgumentException("Unsupported step unit: " + unit);
        }
    }

    private static Object parseIdentityValue(String raw, String duckLakeType)
    {
        switch (duckLakeType.trim().toLowerCase(Locale.ENGLISH)) {
            case "int8":
            case "int16":
            case "int32":
            case "int64":
            case "uint8":
            case "uint16":
            case "uint32":
                return Long.parseLong(raw.trim());
            case "varchar":
                return Slices.utf8Slice(raw);
            case "date":
                return LocalDate.parse(raw.trim()).toEpochDay();
            case "boolean":
                return parseBoolean(raw.trim());
            default:
                // isPrunable() already restricts identity pruning to the types handled above.
                return null;
        }
    }

    private static boolean parseBoolean(String raw)
    {
        if ("true".equalsIgnoreCase(raw)) {
            return true;
        }
        if ("false".equalsIgnoreCase(raw)) {
            return false;
        }
        throw new IllegalArgumentException("Not a boolean partition value: " + raw);
    }

    /**
     * An {@code identity}-transformed partition field with a constraining domain: exact, since the
     * identity transform is a bijection on the value.
     */
    private static final class IdentityCheck
    {
        private final int partitionKeyIndex;
        private final DuckLakeColumnHandle column;
        private final Domain domain;

        IdentityCheck(int partitionKeyIndex, DuckLakeColumnHandle column, Domain domain)
        {
            this.partitionKeyIndex = partitionKeyIndex;
            this.column = requireNonNull(column, "column is null");
            this.domain = requireNonNull(domain, "domain is null");
        }

        boolean keep(DuckLakeDataFile file)
        {
            Optional<String> partitionValue = file.getPartitionValues().get(partitionKeyIndex);
            if (partitionValue == null) {
                return true;
            }
            if (!partitionValue.isPresent()) {
                return domain.isNullAllowed();
            }
            try {
                Object nativeValue = parseIdentityValue(partitionValue.get(), column.getColumnIdentity().getDuckLakeType());
                return nativeValue == null || domain.includesNullableValue(nativeValue);
            }
            catch (RuntimeException e) {
                // Unparseable stored value (or an unexpected shape): never skip the file over it.
                return true;
            }
        }
    }

    /** One calendar-transformed partition field participating in a {@link CalendarGroup}. */
    private static final class CalendarMember
    {
        private final int partitionKeyIndex;
        private final PartitionTransform transform;

        CalendarMember(int partitionKeyIndex, PartitionTransform transform)
        {
            this.partitionKeyIndex = partitionKeyIndex;
            this.transform = requireNonNull(transform, "transform is null");
        }
    }

    /**
     * All the calendar-kind ({@code year}/{@code month}/{@code day}/{@code hour}) partition fields
     * declared on one source column, checked together as a single tuple. A lone field degenerates
     * to a tuple of one, which is exactly the plain per-field check; grouping only matters, and
     * only helps, when several calendar transforms share a source column.
     */
    private static final class CalendarGroup
    {
        private final List<CalendarMember> members;
        private final Domain domain;
        private final boolean allowAll;
        private final Set<List<Long>> allowedTuples;
        private final List<long[]> yearIntervals;
        private final int yearMemberIndex;

        private CalendarGroup(List<CalendarMember> members, Domain domain, boolean allowAll, Set<List<Long>> allowedTuples, List<long[]> yearIntervals)
        {
            this.members = ImmutableList.copyOf(members);
            this.domain = requireNonNull(domain, "domain is null");
            this.allowAll = allowAll;
            this.allowedTuples = allowedTuples;
            this.yearIntervals = ImmutableList.copyOf(yearIntervals);
            this.yearMemberIndex = indexOfYearMember(this.members);
        }

        private static int indexOfYearMember(List<CalendarMember> members)
        {
            for (int i = 0; i < members.size(); i++) {
                if (members.get(i).transform.getKind() == PartitionTransform.Kind.YEAR) {
                    return i;
                }
            }
            return -1;
        }

        /**
         * Builds a group's pruning plan. A bounded range within the cap is enumerated into exact
         * tuples as before. A range that cannot be cheaply enumerated (an unbounded side, or a
         * span at least as wide as the cap) is either dropped to "allow everything" (a group with
         * no {@code year} member: {@code month}/{@code day}/{@code hour} alone give up no more
         * information about it), or, when the group has a {@code year} member, turned into a year
         * interval instead: {@code year} is monotonic, so {@code [year(low) or -inf, year(high) or
         * +inf]} is still exact even when the month/day/hour side of the range cannot be
         * enumerated. This is what lets the common half-open predicate ({@code ts >= ...} or
         * {@code ts < ...}) still prune a {@code month(ts), year(ts)} table by year.
         */
        static CalendarGroup build(List<CalendarMember> members, Domain domain, Type sourceType)
        {
            ValueSet valueSet = domain.getValues();
            if (!(valueSet instanceof SortedRangeSet)) {
                // Not expected for the orderable DATE/TIMESTAMP types calendar transforms apply to,
                // but if it happens there is no exact way to prune: keep everything.
                return new CalendarGroup(members, domain, true, ImmutableSet.of(), ImmutableList.of());
            }

            boolean hasYear = members.stream().anyMatch(member -> member.transform.getKind() == PartitionTransform.Kind.YEAR);
            ChronoUnit unit = finestUnit(members);
            int cap = capFor(unit);
            Set<List<Long>> tuples = new HashSet<>();
            List<long[]> yearIntervals = new ArrayList<>();
            for (Range range : valueSet.getRanges().getOrderedRanges()) {
                if (range.isLowUnbounded() || range.isHighUnbounded()) {
                    if (!hasYear) {
                        return new CalendarGroup(members, domain, true, ImmutableSet.of(), ImmutableList.of());
                    }
                    long lowYear = range.isLowUnbounded() ? Long.MIN_VALUE : toLocalDateTime(range.getLowBoundedValue(), sourceType).getYear();
                    long highYear = range.isHighUnbounded() ? Long.MAX_VALUE : toLocalDateTime(range.getHighBoundedValue(), sourceType).getYear();
                    yearIntervals.add(new long[] {lowYear, highYear});
                    continue;
                }
                LocalDateTime low = toLocalDateTime(range.getLowBoundedValue(), sourceType);
                LocalDateTime high = toLocalDateTime(range.getHighBoundedValue(), sourceType);
                if (unit.between(low, high) >= cap) {
                    if (!hasYear) {
                        return new CalendarGroup(members, domain, true, ImmutableSet.of(), ImmutableList.of());
                    }
                    yearIntervals.add(new long[] {low.getYear(), high.getYear()});
                    continue;
                }
                LocalDateTime cursor = truncateTo(low, unit);
                for (int step = 0; step <= cap && !cursor.isAfter(high); step++) {
                    tuples.add(tupleAt(members, cursor));
                    cursor = cursor.plus(1, unit);
                }
            }
            return new CalendarGroup(members, domain, false, tuples, yearIntervals);
        }

        private static ChronoUnit finestUnit(List<CalendarMember> members)
        {
            boolean hasHour = members.stream().anyMatch(member -> member.transform.getKind() == PartitionTransform.Kind.HOUR);
            boolean hasDay = members.stream().anyMatch(member -> member.transform.getKind() == PartitionTransform.Kind.DAY);
            boolean hasMonth = members.stream().anyMatch(member -> member.transform.getKind() == PartitionTransform.Kind.MONTH);
            if (hasHour) {
                return ChronoUnit.HOURS;
            }
            if (hasDay) {
                return ChronoUnit.DAYS;
            }
            if (hasMonth) {
                return ChronoUnit.MONTHS;
            }
            return ChronoUnit.YEARS;
        }

        private static List<Long> tupleAt(List<CalendarMember> members, LocalDateTime cursor)
        {
            List<Long> tuple = new ArrayList<>(members.size());
            for (CalendarMember member : members) {
                tuple.add(member.transform.apply(cursor));
            }
            return tuple;
        }

        boolean keep(DuckLakeDataFile file)
        {
            if (allowAll) {
                return true;
            }
            List<Long> tuple = new ArrayList<>(members.size());
            for (CalendarMember member : members) {
                Optional<String> partitionValue = file.getPartitionValues().get(member.partitionKeyIndex);
                if (partitionValue == null) {
                    return true;
                }
                if (!partitionValue.isPresent()) {
                    // The source value was NULL, so every field derived from it is NULL too; all of
                    // this group's fields share one domain, so any member answers the question.
                    return domain.isNullAllowed();
                }
                try {
                    tuple.add(Long.parseLong(partitionValue.get().trim()));
                }
                catch (NumberFormatException e) {
                    return true;
                }
            }
            if (allowedTuples.contains(tuple)) {
                return true;
            }
            if (yearMemberIndex < 0 || yearIntervals.isEmpty()) {
                return false;
            }
            long fileYear = tuple.get(yearMemberIndex);
            for (long[] interval : yearIntervals) {
                if (fileYear >= interval[0] && fileYear <= interval[1]) {
                    return true;
                }
            }
            return false;
        }
    }
}
