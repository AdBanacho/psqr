package com.persequor.broker;

import com.persequor.compliance.eu.ComplianceService;
import com.persequor.model.Event;

public class SubscriptionListener implements EventListener {
	@Override
	public void handle(Event event, int deliveryTag) {
		// Send events to the compliance service matching the source ("EU" or "US")
		// Acknowledge event against the queue
	}
}
