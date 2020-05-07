package com.persequor.broker;

import com.persequor.EventSerializer;
import com.persequor.repository.EventRepository;
import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class EventProcessorTest {
	@Mock
	private EventRepository repository;
	@Mock
	private EventSerializer serializer;
	@Mock
	private EventQueue eventQueue;

	private EventPersister eventProcessor;

	@Before
	public void setup() {
		MockitoAnnotations.initMocks(this);

		eventProcessor = new EventPersister(repository, eventQueue);
	}

	/// TODO: Implement tests as needed
}
