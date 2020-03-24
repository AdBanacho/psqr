package com.persequor.repository;

import java.time.LocalDate;

/**
 * Do not implement the repository, somebody else will do that. You may change the interface though
 */
public interface StatisticsRepository {
	void updateStatistics(LocalDate day, int numberOfTrackedIds);
}
