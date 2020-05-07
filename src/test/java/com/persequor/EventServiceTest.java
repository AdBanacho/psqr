package com.persequor;


import com.persequor.broker.EventQueue;
import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class EventServiceTest {

    private EventService service;
    @Mock
    private EventSerializer eventSerializer;
    @Mock
    private EventQueue eventQueue;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        service = new EventService(eventSerializer, eventQueue);
    }

    /*
     * Write all the tests you need
    @Test
    public void someCleverTestName() throws Throwable
    {
        service.process("{id:'4ba28c38-6c9b-4926-b6f5-0d45794fc41f', created_at: '2018-01-12T13:37:30Z', source: 'interweb', action: 'take-over'}");
    }
    */

    /// TODO: Implement tests as needed
}