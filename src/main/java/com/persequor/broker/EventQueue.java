package com.persequor.broker;

public interface EventQueue {
	void push(String event);
	void acknowledge(String event);
	void addListener(EventListener listener);
}
