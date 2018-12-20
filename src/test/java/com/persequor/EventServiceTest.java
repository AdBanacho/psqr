package com.persequor;


import com.persequor.repository.EventRepository;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class EventServiceTest {

    private EventService service;
    @Mock
    private EventSerializer serializer;
    @Mock
    private EventRepository repository;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        service = new EventService(serializer, repository);
    }

    /*
     * Write all the tests you need
    @Test
    public void someCleverTestName() throws Throwable
    {
        service.process("{id:'4ba28c38-6c9b-4926-b6f5-0d45794fc41f', created_at: '2018-01-12T13:37:30Z', source: 'interweb', action: 'take-over'}");
    }
    */
}