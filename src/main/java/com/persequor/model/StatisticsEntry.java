package com.persequor.model;

import java.time.LocalDate;
import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

public final class StatisticsEntry {
    private final UUID id;
    private final LocalDate day;
    private final int itemCount;

    public StatisticsEntry(UUID id, LocalDate day, int itemCount) {
        this.id = id;
        this.day = day;
        this.itemCount = itemCount;
    }

    public UUID getId() {
        return id;
    }

    public LocalDate getDay() {
        return day;
    }

    public int getItemCount() {
        return itemCount;
    }

    /** Groups entries whose processing keys were newly inserted in the current transaction. */
    public static Map<LocalDate, Long> totals(Collection<StatisticsEntry> newEntries) {
        Map<LocalDate, Long> totals = new TreeMap<>();
        for (StatisticsEntry entry : newEntries) {
            totals.merge(entry.day, (long) entry.itemCount, Math::addExact);
        }
        return totals;
    }
}
