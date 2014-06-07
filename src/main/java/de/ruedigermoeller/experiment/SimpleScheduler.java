package de.ruedigermoeller.experiment;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.locks.LockSupport;

/**
 * Created by ruedi on 07.06.14.
 */
public class SimpleScheduler {

    private ThreadPoolExecutor executor;
    private DynamicDisruptor disruptor;

    static class SimpleEventEntry {
        public long worknanos;
        public boolean done = true;                   // true if has been processed
        public boolean debugSeen = true;              // true if has been seen

        public void work() {
            long sum = 0;
            long max = worknanos / 10;
            for (int i = 0; i < max; i++ ) {
                sum += i;
            }
            if ( Math.abs(sum) < 88 ) {
                System.out.println("POK");
            }
//            LockSupport.parkNanos(worknanos);
        }
    }

    enum SimpleDispacherState {
        UNSCHEDULED, // not used
        IN_ADD, // added but not used
        SCHEDULED, // runs normally
        IN_REMOVE, // trying to remove
    }

    class SimpleDispatcherHandler implements EventHandler<SimpleEventEntry> {

        long load = 0;                      // acummulated load of handler

        int profileTick = 0;                // counter when to profile again
        int eventTick = 0;                  // ticker incremented for each event happening
        int scheduleTick = 0;               // counter when to check actor rescheduling
        int emptyCount = 0;
        int num;
        int dispatcherScheduleTick = 0;
        SimpleDispatcherHandler handlers[];
        private SimpleDispacherState state = SimpleDispacherState.UNSCHEDULED;

        SimpleDispatcherHandler(int num, SimpleDispatcherHandler[] handlers) {
            this.num = num;
            this.handlers = handlers;
        }

        @Override
        public String toString() {
            return "DispatcherHandler{" +
                    "state=" + state +
                    "num=" + num +
                    ", load=\t" + load +
                    '}';
        }

        @Override
        public void onEvent(SimpleEventEntry ev, long sequence, boolean endOfBatch) throws Exception {
            if ( state == SimpleDispacherState.IN_ADD ) {
                dispatcherIndex++;
                state = SimpleDispacherState.SCHEDULED;
                System.out.println("scheduled dispatcher " + (dispatcherIndex - 1));
            }
            if ( ! ev.done ) {
                ev.debugSeen = true;
                eventTick++;
                // standard processing
                ev.done = true;
                processEvent(ev);

                if (state == SimpleDispacherState.IN_REMOVE) {
                    if (num < dispatcherIndex - 1) {
                        // new thread has been scheduled meanwhile
                        state = SimpleDispacherState.SCHEDULED;
                        System.out.println("remove reverted, another thread started " + num);
                    } else if (num >= dispatcherIndex) {
                        // num >= dispatcherIndex => another remove has happened. Error
                        System.out.println("this should never happen");
                        System.exit(-1);
                    } else {
                        removeHandler();
                    }
                }
                if (eventTick > 500) {
                    load = 4 * load / 5; // fadeout
                    eventTick = 0;
                }
            }
        }

        private void removeHandler() {
            dispatcherIndex--;
            System.out.println("removed " + num);
            state = SimpleDispacherState.UNSCHEDULED;
            disruptor.removeHandler(this);
        }

        private void processEvent(SimpleEventEntry ev) {
            profileTick++;
            if ( profileTick > 100 ) {
                profileTick = 0;
                profiledRun(ev);
            } else {
                ev.work();
            }
        }

        private void profiledRun(SimpleEventEntry ev) {
            // profile
            long tim = System.nanoTime();
            ev.work();
            long dur = System.nanoTime() - tim;
            load += dur;
            scheduleTick++;
            if ( scheduleTick > 4 ) {

                scheduleTick = 0;
                if (num == 0) {
                    dispatcherScheduleTick++;
                    if (dispatcherScheduleTick > 400) {
                        // if rebalancing does not help (800 rounds of rebalance did not fix q)
                        // and queue keeps growing => try get another thread
                        if (!ringBuffer.hasAvailableCapacity(ringBuffer.getBufferSize() / 2)) {
                            System.out.println("try schedule dispatcher " + dispatcherIndex);
                            scheduleDispatcher(false);
                        }
                        dispatcherScheduleTick = 0;
                    }
                }

                // try removing load
                if (num > 0 && num == dispatcherIndex-1 && state != SimpleDispacherState.IN_ADD ) {
                    if ( ringBuffer.hasAvailableCapacity(ringBuffer.getBufferSize()*2/3) ) {
                        emptyCount++;
                        if (emptyCount > 100) {
                            emptyCount = 0;
                            state = SimpleDispacherState.IN_REMOVE;
                        }
                    } else
                        emptyCount = 0;
                }
            }
        }

        private void dumpWorkers() {
            for (int i = 0; i < handlers.length; i++) {
                SimpleDispatcherHandler handler = handlers[i];
                System.out.println(handler);
            }
        }

    }

    SimpleDispatcherHandler dispatchers[];
    int dispatcherIndex = 1;
    RingBuffer<SimpleEventEntry> ringBuffer;

    void initDisruptor() {
        int MAX_THREADS = 16;
        executor = (ThreadPoolExecutor) Executors.newCachedThreadPool();
        disruptor = new DynamicDisruptor( () -> new SimpleEventEntry(), 1024*512); // requires 4 MB l3 cache
        dispatchers = new SimpleDispatcherHandler[MAX_THREADS];
        for (int i = 0; i < dispatchers.length; i++) {
            dispatchers[i] = new SimpleDispatcherHandler(i,dispatchers);
        }

        ringBuffer = disruptor.getRingBuffer();
        CountDownLatch latch = new CountDownLatch(1);
        executor.execute(() -> disruptor.addHandler(dispatchers[0],latch));
        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    void scheduleDispatcher(boolean latched) {
        if ( dispatcherIndex < dispatchers.length ) {
            final int di = dispatcherIndex;
            if ( dispatchers[di].state == SimpleDispacherState.IN_ADD ) {
                System.out.println("schedule already underway "+di);
                return;
            }
            dispatchers[di].state = SimpleDispacherState.IN_ADD;
            dispatchers[di].emptyCount = 0;
            CountDownLatch latch = latched ? new CountDownLatch(1) : null;
            executor.execute( () -> disruptor.addHandler(dispatchers[di],latch) );
            if ( latched ) {
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                dispatcherIndex++;
                dispatchers[di].state = SimpleDispacherState.SCHEDULED;
                System.out.println("scheduled dispatcher " + (dispatcherIndex - 1));
            }
        }
    }

    public void enqueueReq(int nr, long nanos) {
        final long seq = ringBuffer.next();
        final SimpleEventEntry requestEntry = ringBuffer.get(seq);
        if ( ! requestEntry.done ) {
            System.out.println("unprocessed event !");
            System.exit(1);
        }
        requestEntry.done = false;
        requestEntry.debugSeen = false;
        requestEntry.worknanos = nanos;
        ringBuffer.publish(seq);
    }

    public static void main(String arg[]) {
        SimpleScheduler sched = new SimpleScheduler();
        sched.initDisruptor();
        long tim = System.currentTimeMillis();
        int count = 0;
        int speed = 1;
        while( true ) {
            int actorId = (int) (Math.random() * 64);
            sched.enqueueReq(actorId, 250 * (10 + actorId * 10)); //*actorId
            if ( (count%speed) == 0 ) {
                LockSupport.parkNanos(100);
            }
            count++;
            long diff = System.currentTimeMillis() - tim;
            int slowCount = 0;
            if ( diff > 1000 ) {
                System.out.println("Count:"+count*1000/diff+" "+diff+" spd "+speed);
//                if ( (count%2) == 0 )

                speed++;
//                speed = speed + ((int)(Math.random()*11) - 5);
//                if ( speed > 70 ) {
//                    speed = 1;
//                    slowCount = 60; // 60 sec speed 1
//                }
//                if ( speed < 1 || slowCount > 0 ) {
//                    speed = 1;
//                    slowCount--;
//                }

                count = 0;
                tim = System.currentTimeMillis();
            }
        }
    }

}
