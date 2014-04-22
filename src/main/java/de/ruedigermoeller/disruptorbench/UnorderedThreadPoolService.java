package de.ruedigermoeller.disruptorbench;

import de.ruedigermoeller.serialization.FSTConfiguration;

import java.io.IOException;
import java.util.concurrent.*;
import java.util.concurrent.locks.LockSupport;

/**
 * Created by ruedi on 21.04.14.
 */
public class UnorderedThreadPoolService implements Service {

    static FSTConfiguration conf = FSTConfiguration.getDefaultConfiguration();
    private LoadFeeder serv;
    private ExecutorService executor;

    private static class TestThread extends Thread {
        public TestRequestEntry req = new TestRequestEntry();
        public TestThread(Runnable r, String name) {
            super(r,name);
        }
    }

    ThreadLocal<LoadFeeder.Response> respTLocal = new ThreadLocal() {
        @Override
        protected LoadFeeder.Response initialValue() {
            return new LoadFeeder.Response(null,0);
        }
    };

    SharedData sharedData;

    public UnorderedThreadPoolService(LoadFeeder serv, SharedData data, int numWorkers) {
        this.serv = serv;
        init(numWorkers);
        sharedData = data;
    }

    public void init(int workers) {
        executor = (ExecutorService) createBoundedThreadExecutor(workers, "pool", 40000);
//        executor = Executors.newFixedThreadPool(workers,new ThreadFactory() {
//            @Override
//            public Thread newThread(Runnable r) {
//                return new TestThread( r, "-" );
//            }
//        });
    }

    @Override
    public void processRequest(final byte[] rawRequest) {
        executor.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    final TestRequestEntry testRequest = ((TestThread)Thread.currentThread()).req;
                    testRequest.rawRequest = rawRequest;
                    testRequest.decode();
                    testRequest.process(sharedData);
                    testRequest.encode(serv);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }

    @Override
    public void shutdown() {
        executor.shutdownNow();
        while ( ((ThreadPoolExecutor)executor).getActiveCount() > 0 )
            LockSupport.parkNanos(1000*1000);
    }

    public static Executor createBoundedThreadExecutor(int workers, final String name, int qsize) {
        ThreadPoolExecutor res = new ThreadPoolExecutor(workers,workers,1000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(qsize));
//        ThreadPoolExecutor res = new ThreadPoolExecutor(1,1,1000, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(qsize));
        res.setThreadFactory(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                return new TestThread( r, name );
            }
        });
        res.setRejectedExecutionHandler(new RejectedExecutionHandler() {
            @Override
            public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                while ( !executor.isShutdown() && !executor.getQueue().offer(r)) {
                    LockSupport.parkNanos(1000);
                }
            }
        } );
        return res;
    }

    public static void main(String a[]) throws IOException, ClassNotFoundException {
        int WARMUP = 5;
        int RUN = 10;
        for ( int ii=1; ii < 9; ii++ ) {
            long sum = 0;
            for (int i = 0; i < RUN + WARMUP; i++) {
                LoadFeeder feeder = new LoadFeeder(10000);
//                UnorderedThreadPoolService service = new UnorderedThreadPoolService(feeder, new LockFreeSharedData(), ii);
                UnorderedThreadPoolService service = new UnorderedThreadPoolService(feeder,new SynchronizedSharedData(), ii);
                long run = feeder.run(service, 1000 * 1000);
                if (i > WARMUP) {
                    sum += run;
                }
            }
            System.out.println("result ("+ii+"):" + (sum / RUN));
        }
    }

}
