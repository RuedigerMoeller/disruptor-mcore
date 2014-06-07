package de.ruedigermoeller.experiment;

import com.lmax.disruptor.*;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

/**
 * Created by ruedi on 06.06.14.
 */
public class DynamicDisruptor<T> {
    private final RingBuffer<T> ringBuffer;
    private final SequenceGroup sequenceGroup = new SequenceGroup();
    private final Map<EventHandler<T>, BatchEventProcessor<T>> handlersWithProcessors = new ConcurrentHashMap<>();

    public DynamicDisruptor(EventFactory<T> fac, int bufferSize) {
        ringBuffer = RingBuffer.createMultiProducer(fac, bufferSize, new SleepingWaitStrategy());
        ringBuffer.addGatingSequences(sequenceGroup);
    }

    public void addHandler(EventHandler<T> handler, CountDownLatch latch) {
        SequenceBarrier barrier = ringBuffer.newBarrier();
        BatchEventProcessor<T> processor = new BatchEventProcessor<>(ringBuffer, barrier, handler);
        processor.getSequence().set(barrier.getCursor());
        sequenceGroup.add(processor.getSequence());
        processor.getSequence().set(ringBuffer.getCursor());
        handlersWithProcessors.put(handler, processor);
        if ( latch != null ) {
            latch.countDown();
        }
        processor.run();
    }

    public void removeHandler(EventHandler<T> handler) {
        BatchEventProcessor<T> processor = handlersWithProcessors.remove(handler);
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
