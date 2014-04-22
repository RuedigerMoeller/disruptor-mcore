package de.ruedigermoeller.disruptorbench;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SleepingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

/**
 * Created by ruedi on 20.04.14.
 */
public class DisruptorService implements Service {

    static AtomicInteger decount = new AtomicInteger(0); // debug

    public static class DSEventFac implements EventFactory<TestRequestEntry> {
        @Override
        public TestRequestEntry newInstance() {
            return new TestRequestEntry();
        }
    }

    public static class DSPartitionedProcessor implements EventHandler<TestRequestEntry> {
        int part;

        public DSPartitionedProcessor(int part) {
            this.part = part;
        }

        @Override
        public void onEvent(TestRequestEntry event, long sequence, boolean isEndOfBatch) throws Exception {
            if ( event.decPartition == part )
                event.decode();
        }
    }

    Disruptor<TestRequestEntry> disruptor;
    static ExecutorService executor;
    SharedData sharedData;

    int decPartCount = 0;
    int encPartCount = 0;
    int numDecodingThreads;
    int numEncodingThreads;

    public DisruptorService(LoadFeeder feeder, int numDecodingThreads, int numEncodingThreads, SharedData sharedData) {
        this.numDecodingThreads = numDecodingThreads;
        this.numEncodingThreads = numEncodingThreads;
        this.sharedData = sharedData;
        initDisruptor(feeder);
    }

    void initDisruptor(final LoadFeeder feeder) {
        if ( executor == null )
            executor = Executors.newCachedThreadPool();
        disruptor = new Disruptor<>(new DSEventFac(), 32768, executor, ProducerType.SINGLE, new SleepingWaitStrategy());
//        DSPartitionedProcessor decoders[] = new DSPartitionedProcessor[codingThreads];
        DSPartitionedProcessor decoders[] = new DSPartitionedProcessor[numDecodingThreads];
        for (int i = 0; i < decoders.length; i++) {
            decoders[i] = new DSPartitionedProcessor(i);
        }
        DSPartitionedProcessor encoders[] = new DSPartitionedProcessor[numEncodingThreads];
        for (int i = 0; i < encoders.length; i++) {
            encoders[i] = new DSPartitionedProcessor(i) {
                @Override
                public void onEvent(TestRequestEntry event, long sequence, boolean isEndOfBatch) throws Exception {
                    if ( event.encPartition == part )
                        event.encode(feeder);
                }
            };
        }
        disruptor
                .handleEventsWith(decoders)
                .then(new EventHandler<TestRequestEntry>() {
                    @Override
                    public void onEvent(TestRequestEntry event, long sequence, boolean endOfBatch) throws Exception {
                        event.process(sharedData);
                    }
                })
                .handleEventsWith(encoders)
        ;
        disruptor.start();
    }

    @Override
    public void processRequest(byte[] b) {
        final RingBuffer<TestRequestEntry> ringBuffer = disruptor.getRingBuffer();
        final long seq = ringBuffer.next();
        final TestRequestEntry requestEntry = ringBuffer.get(seq);
        requestEntry.rawRequest = b;
        requestEntry.decPartition = decPartCount++;
        requestEntry.encPartition = encPartCount++;
        if ( decPartCount == numDecodingThreads )
            decPartCount = 0;
        if ( encPartCount == numEncodingThreads )
            encPartCount = 0;
        ringBuffer.publish(seq);
    }

    @Override
    public void shutdown() {
        disruptor.shutdown();
        while ( ((ThreadPoolExecutor)executor).getActiveCount() > 0 )
            LockSupport.parkNanos(1000*1000);
//        executor.shutdownNow();
    }

    public static void main(String a[]) throws IOException, ClassNotFoundException {
        
        int threadDistributions2Test[][] = {
                { 1, 1 },
                { 2, 1 },
                { 2, 2 },
                { 3, 2 },
                { 3, 3 },
                { 4, 3 },
                { 4, 4 },
        };

        int WARMUP = 5;
        int RUN = 10;
        for (int ii = 0; ii < threadDistributions2Test.length; ii++) {
            int[] threads = threadDistributions2Test[ii];
            long sum = 0;
            for ( int i = 0; i < RUN+WARMUP; i++ ) {
                LoadFeeder feeder = new LoadFeeder(10000);
                DisruptorService service = new DisruptorService(feeder, threads[0], threads[1], new SingleThreadedSharedData());
                long run = feeder.run(service, 1000 * 1000);
                if (i > WARMUP) {
                    sum+=run;
                }
            }
            System.out.println("result: (dec:"+threads[0]+", enc:"+threads[1]+")"+(sum/RUN));
        }
    }


}
