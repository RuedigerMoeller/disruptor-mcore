package de.ruedigermoeller.experiment;

import com.lmax.disruptor.*;

import java.lang.reflect.Executable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * Created by ruedi on 06.06.14.
 */
public class DynamicDisruptor<T> {
    private final RingBuffer<T> ringBuffer;
    private final SequenceGroup sequenceGroup = new SequenceGroup();
    private final Map<EventHandler<T>, MyBatchEventProcessor<T>> handlersWithProcessors = new ConcurrentHashMap<>();

    public Executor feedBackQueue = Executors.newSingleThreadExecutor();

    public DynamicDisruptor(EventFactory<T> fac, int bufferSize) {
        ringBuffer = RingBuffer.createMultiProducer(fac, bufferSize, new BusySpinWaitStrategy());
        ringBuffer.addGatingSequences(sequenceGroup);
    }

    SequenceBarrier barrier = null;
    public void addHandler(EventHandler<T> handler, CountDownLatch latch) {
//        SequenceBarrier barrier = null;
        if (barrier==null) {
            barrier = ringBuffer.newBarrier();
        }
        MyBatchEventProcessor<T> processor = new MyBatchEventProcessor<>(ringBuffer, barrier, handler);
        processor.getSequence().set(barrier.getCursor());
        sequenceGroup.add(processor.getSequence());
        processor.getSequence().set(ringBuffer.getCursor());
        handlersWithProcessors.put(handler, processor);
        if ( latch != null ) {
            latch.countDown();
        }
        processor.run();
    }

    public MyBatchEventProcessor<T> getProcessor(EventHandler<T> handler) {
        return handlersWithProcessors.get(handler);
    }

    public void removeHandler(EventHandler<T> handler) {
        MyBatchEventProcessor<T> processor = handlersWithProcessors.remove(handler);
        processor.halt();
        sequenceGroup.remove(processor.getSequence());
    }

    public boolean hasHandlers() {
        return handlersWithProcessors.size() != 0;
    }

    public RingBuffer<T> getRingBuffer() {
        return ringBuffer;
    }
}
