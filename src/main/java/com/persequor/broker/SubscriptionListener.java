package com.persequor.broker;

import com.persequor.compliance.eu.ComplianceService;
import com.persequor.model.Event;

public class SubscriptionListener implements EventListener {
	@Override
	public void handle(Event event, int deliveryTag) {
		// TODO: Send events to the compliance service matching the source ("EU" or "US")
		// TODO: Acknowledge event against the queue
	}
}
