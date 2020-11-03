package com.persequor.compliance.eu;

import java.time.ZonedDateTime;
import java.util.Set;

public class ComplianceMessage {
	private String messageId;
	private ZonedDateTime occurrence;
	private Action action;
	private Set<String> items;

	public String getMessageId() {
		return messageId;
	}

	public void setMessageId(String messageId) {
		this.messageId = messageId;
	}

	public ZonedDateTime getOccurrence() {
		return occurrence;
	}

	public void setOccurrence(ZonedDateTime occurrence) {
		this.occurrence = occurrence;
	}

	public Action getAction() {
		return action;
	}

	public void setAction(Action action) {
		this.action = action;
	}

	public Set<String> getItems() {
		return items;
	}

	public void setItems(Set<String> items) {
		this.items = items;
	}
}
