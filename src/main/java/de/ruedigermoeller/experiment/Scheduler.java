package de.ruedigermoeller.experiment;

import com.lmax.disruptor.*;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.locks.LockSupport;

/**
 * Created by ruedi on 06.06.14.
 */
public class Scheduler {

    private ThreadPoolExecutor executor;
    private DynamicDisruptor disruptor;

    static class EventEntry {
        public long id;
        public int nr=99;                             // marker for init
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

    static class DispatcherThread extends Thread {

        public DispatcherThread(String name) {
            super(name);
        }

    }

    enum DispacherState {
        UNSCHEDULED, // not used
        IN_ADD, // added but not used
        SCHEDULED, // runs normally
        IN_REMOVE, // trying to remove
    }

    class DispatcherHandler implements EventHandler<EventEntry> {

        public long idMask;             // bitmask which actors to process
        public long transitionalIdMask; // bitmask which actors are about to leave

        long timeCounters[] = new long[64]; // holds profiling data
        long load = 0;                      // acummulated load of handler

        int profileTick = 0;                // counter when to profile again
        int eventTick = 0;                  // ticker incremented for each event happening
        int scheduleTick = 0;               // counter when to check actor rescheduling
        int emptyCount = 0;
        int num;
        int dispatcherScheduleTick = 0;
        DispatcherHandler handlers[];
        private DispacherState state = DispacherState.UNSCHEDULED;

        DispatcherHandler(int num, DispatcherHandler[] handlers) {
            this.num = num;
            this.handlers = handlers;
        }

        @Override
        public String toString() {
            return "DispatcherHandler{" +
                    "state=" + state +
                    "num=" + num +
                    ", load=\t" + load +
                    ", idMask=\t" + Long.toBinaryString(idMask) +
                    '}';
        }

        @Override
        public void onEvent(EventEntry ev, long sequence, boolean endOfBatch) throws Exception {
            if ( state == DispacherState.IN_ADD ) {
                dispatcherIndex++;
                state = DispacherState.SCHEDULED;
                System.out.println("scheduled dispatcher " + (dispatcherIndex - 1));
            }
            if ( ev.done )
                return;
            ev.debugSeen = true;
            eventTick++;
            if ( ((idMask & ev.id) != 0 && ! ev.done) ) {
                // standard processing
                ev.done = true;
                processEvent(ev);
            } else if ( ((transitionalIdMask & ev.id) != 0 ) ) {
                if ( ev.done ) // has been processed by new handler
                {
                    // clear from transitional bit array. as further
                    // events are handled by newly assigned one
                    transitionalIdMask = transitionalIdMask & ~ev.id;
                } else {
                    // matched transitional, keep processing events from
                    // old events until new one took over
                    ev.done = true;
                    processEvent(ev);
                }
            }

            if (state == DispacherState.IN_REMOVE ) {
                if ( num < dispatcherIndex-1 ) {
                    // new thread has been scheduled meanwhile
                    state = DispacherState.SCHEDULED;
                    System.out.println("remove reverted, another thread started " + num);
                } else if (num >= dispatcherIndex ) {
                    // num >= dispatcherIndex => another remove has happened. Error
                    System.out.println("this should never happen");
                    System.exit(-1);
                } else if ( idMask != 0 ) {
                    // new actors scheduled to this FIXME: what happens if tasks in transition are moved to dying handler, but dying handler does not see yet ?
                    state = DispacherState.SCHEDULED;
                    System.out.println("remove reverted, has new assignments " + num);
                } else if ( transitionalIdMask != 0 ) {
                    // still in transition. do nothing
                } else {
                    if ( transitionalIdMask == 0 && idMask == 0 && num == dispatcherIndex-1 ) {
                        removeHandler();
                    } else {
                        System.out.println("unexpected state");
                        System.exit(-1);
                    }
                }
            }
            if ( eventTick > 500 ) {
                load = 4*load/5; // fadeout
                eventTick = 0;
            }
        }

        private void removeHandler() {
            dispatcherIndex--;
            System.out.println("removed " + num);
            state = DispacherState.UNSCHEDULED;
            disruptor.removeHandler(this);
        }

        private void processEvent(EventEntry ev) {
            profileTick++;
            if ( profileTick > 100 ) {
                profileTick = 0;
                profiledRun(ev);
            } else {
                ev.work();
            }
        }

        private void profiledRun(EventEntry ev) {
            // profile
            long tim = System.nanoTime();
            ev.work();
            long dur = System.nanoTime() - tim;
            load += dur;
            timeCounters[ev.nr] = (dur+timeCounters[ev.nr])/2;
            scheduleTick++;
            if ( scheduleTick > 4 ) {
                //if ( !ringBuffer.hasAvailableCapacity(ringBuffer.getBufferSize()*2/3) )
                {
                    // if queue is filled more than 1/3, dispatch load
                    boolean isMaxThread = true;
                    for (int i = 0; i < dispatcherIndex; i++) {
                        DispatcherHandler handler = handlers[i];
                        if (handler != this && handler.load > load) {
                            isMaxThread = false;
                        }
                    }

                    if (isMaxThread) {
                        long max = Integer.MIN_VALUE;
                        int maxAct = -1;
                        for (int i = 0; i < timeCounters.length; i++) {
                            long callCounter = timeCounters[i];
                            if (callCounter > max) {
                                maxAct = i;
                                max = callCounter;
                            }
                        }
                        if (max > 0) {
                            DispatcherHandler newHandler = findIdleHandler(dispatcherIndex);
                            if (newHandler != this && newHandler != null) {
                                movActor(maxAct, newHandler);
                            }
                        }
                    }

                    scheduleTick = 0;
                    if (num == 0) {
                        dispatcherScheduleTick++;
                        if (dispatcherScheduleTick > 400) {
                            // if rebalancing does not help (800 rounds of rebalance did not fix q)
                            // and queue keeps growing => try get another thread
                            if (!ringBuffer.hasAvailableCapacity(ringBuffer.getBufferSize() / 2)) {
                                scheduleDispatcher(false);
                            }
                            dispatcherScheduleTick = 0;
                        }
                    }
                }

                // try removing load
                if (num > 0 && num == dispatcherIndex-1 && state != DispacherState.IN_ADD ) {
                    if ( ringBuffer.hasAvailableCapacity(ringBuffer.getBufferSize()*2/3) ) {
                        emptyCount++;
                        if (emptyCount > 100) {
                            //                            if ( state == DispacherState.IN_REMOVE )
                            //                                System.out.println("pending remove");
                            //                            else
                            //                                System.out.println("try remove "+num);
                            int actor2Move = Long.numberOfTrailingZeros(idMask);
                            while (actor2Move < 64) {
                                DispatcherHandler newHandler = findIdleHandler(dispatcherIndex - 1);
                                if (newHandler != this && newHandler != null) {
                                    movActor(actor2Move, newHandler);
                                    actor2Move = Long.numberOfTrailingZeros(idMask);
                                } else
                                    break;
                            }
                            emptyCount = 0;
                            state = DispacherState.IN_REMOVE;
                        }
                    } else
                        emptyCount = 0;
                }

            }
        }

        public void movActor(int actNum, DispatcherHandler newHandler) {
            idMask &= ~(1l << actNum);
            newHandler.idMask |= 1l << actNum;
            transitionalIdMask |= 1l << actNum;
            timeCounters[actNum] = 0;
        }

        private void dumpWorkers() {
            for (int i = 0; i < handlers.length; i++) {
                DispatcherHandler handler = handlers[i];
                System.out.println(handler);
            }
        }

        private DispatcherHandler findIdleHandler( int dispatcherMaxIndex ) {
//            return dispatchers[dispatcherIndex-1];
            long minLoad = Long.MAX_VALUE;
            DispatcherHandler idlehandler = null;
            for (int i = 0; i < dispatcherMaxIndex; i++) {
                DispatcherHandler handler = handlers[i];
                if ( handler.load < minLoad ) {
                    idlehandler = handler;
                    minLoad = handler.load;
                }
            }
            return idlehandler;
        }

    }

    DispatcherHandler dispatchers[];
    int dispatcherIndex = 1;
    RingBuffer<EventEntry> ringBuffer;

    void initDisruptor() {
        int MAX_THREADS = 16;
        executor = (ThreadPoolExecutor) Executors.newCachedThreadPool();
        disruptor = new DynamicDisruptor( () -> new EventEntry(), 1024*512); // requires 4 MB l3 cache
        dispatchers = new DispatcherHandler[MAX_THREADS];
        for (int i = 0; i < dispatchers.length; i++) {
            dispatchers[i] = new DispatcherHandler(i,dispatchers);
        }
        dispatchers[0].idMask = 0xffffffffffffffffl;

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
            System.out.println("try schedule dispatcher " + dispatcherIndex);
            final int di = dispatcherIndex;
            if ( dispatchers[di].state == DispacherState.IN_ADD ) {
                System.out.println("schedule already underway "+di);
                return;
            }
            dispatchers[di].state = DispacherState.IN_ADD;
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
                dispatchers[di].state = DispacherState.SCHEDULED;
                System.out.println("scheduled dispatcher " + (dispatcherIndex - 1));
            }
        }
    }

    public void enqueueReq(int nr, long nanos) {
        final long seq = ringBuffer.next();
        final EventEntry requestEntry = ringBuffer.get(seq);
        if ( ! requestEntry.done && requestEntry.nr != 99) {
            System.out.println("unprocessed event !");
            System.exit(1);
        }
        requestEntry.nr = nr;
        requestEntry.done = false;
        requestEntry.debugSeen = false;
        requestEntry.id = 1l<<nr;
        requestEntry.worknanos = nanos;
        ringBuffer.publish(seq);
    }

    public static void main(String arg[]) {
        Scheduler sched = new Scheduler();
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
