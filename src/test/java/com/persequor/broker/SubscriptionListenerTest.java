package com.persequor.broker;

import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class SubscriptionListenerTest {
	@Mock
	private EventQueue eventQueue;

	private SubscriptionListener subscriptionListener;

	@Before
	public void setup() {
		MockitoAnnotations.initMocks(this);

		subscriptionListener = new SubscriptionListener();
	}
}
