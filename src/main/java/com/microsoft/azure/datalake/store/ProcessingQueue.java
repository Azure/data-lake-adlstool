package com.microsoft.azure.datalake.store;

import java.util.LinkedList;
import java.util.Queue;

class ProcessingQueue<T> {
    private Queue<T> internalQueue;
    private int processorCount = 0;
    
    ProcessingQueue() {
    	internalQueue = new LinkedList<T>();
    }

    public synchronized void add(T item) {
        if (item == null) throw new IllegalArgumentException("Cannot put null into queue");
        internalQueue.add(item);
        this.notify();
    }

    public synchronized T poll() {
        try {
            while (isQueueEmpty() && !done())
                wait();
            if (!isQueueEmpty()) {
                processorCount++;  // current thread is now processing the item we pop
                return internalQueue.poll();
            }
            if (done()) {
                return null;
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
        return null; // just to keep the compiler happy - it couldn't infer that all code-paths are covered above.
    }

    public synchronized void unregister() {
        processorCount--;
        if (processorCount < 0) {
            throw new IllegalStateException("too many unregister()'s. processorCount is now " + processorCount);
        }
        if (done()) this.notifyAll();
    }

    public boolean done() {
        return (processorCount == 0 && isQueueEmpty());
    }

    private boolean isQueueEmpty() {
        return (internalQueue.peek() == null);
    }
}
