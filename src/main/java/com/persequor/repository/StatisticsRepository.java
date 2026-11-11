package com.persequor.repository;

import com.persequor.model.StatisticsEntry;

import java.util.List;

public interface StatisticsRepository {
    /** Commits new event keys and their daily increments together. Already-counted UUIDs are skipped. */
    void apply(List<StatisticsEntry> entries);
}
