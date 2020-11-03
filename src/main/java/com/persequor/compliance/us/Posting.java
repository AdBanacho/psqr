package com.persequor.compliance.us;

import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Set;

public class Posting {
	private String postingId;
	private LocalHour occurrence;
	private Activity action;
	private List<ID> items;

	public String getPostingId() {
		return postingId;
	}

	public void setPostingId(String postingId) {
		this.postingId = postingId;
	}

	public LocalHour getOccurrence() {
		return occurrence;
	}

	public void setOccurrence(LocalHour occurrence) {
		this.occurrence = occurrence;
	}

	public Activity getAction() {
		return action;
	}

	public void setAction(Activity action) {
		this.action = action;
	}

	public List<ID> getItems() {
		return items;
	}

	public void setItems(List<ID> items) {
		this.items = items;
	}
}
