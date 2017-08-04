package com.microsoft.azure.datalake.store;
import java.util.Queue;

class ConsumerQueue<T> {
	Queue<T> dataQ;
	private boolean producerActive;
	int capacity = 1000 * 1000;
	
	ConsumerQueue(Queue<T> inQ) {
		dataQ = inQ;
		producerActive = true;
	}
	
	public synchronized void markComplete() {
		producerActive = false;
		notifyAll();
	}
	
	public synchronized void add(T item) {
		while(dataQ.size() >= capacity) {
			try {
				wait();
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		}
		dataQ.add(item);
		notify();
	}
	
	public synchronized T poll() {
		try {
			while(producerActive && isEmpty()) {
				wait();
			}
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
		T val = dataQ.poll();
		notify();
		return val;
	}
	
	public synchronized boolean isEmpty() {
		return dataQ.isEmpty();
	}

}
