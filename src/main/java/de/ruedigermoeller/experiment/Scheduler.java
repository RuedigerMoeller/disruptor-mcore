package de.ruedigermoeller.experiment;

import com.lmax.disruptor.*;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.locks.LockSupport;

/**
 * Created by ruedi on 06.06.14.
 */
public class Scheduler {

    public static final int PROFILE_THRESH  = 1000;                // when to run a call profiled (N'th) call
    public static final int REBALANCE_PER_PROFILETICK = 100;        // each N profile, do a rebalance on actors amongst live threads.
    public static final int EMPTY_QUEUE_TICK_THRESH_FOR_REMOVE_THREAD = 100; // how many times in a row 'empty' queue must occur to trigger remove
    public static final int FULL_QUEUE_TICK_THRESH_FOR_ADD_THREAD = 10; // how many times in a row 'full' queue must occur to trigger add

    public static final int EVENTTICK_RESET = 10000;  // when to akkumulate overall load
    public static final boolean BALANCE_DEBUG = false;
    public static final int MAX_THREADS = 16;

    private ThreadPoolExecutor executor;
    private DynamicDisruptor disruptor;

    static class EventEntry {

        public long actorsToMove;
        public int  targetNum = -1;

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

    enum DispacherState {
        UNSCHEDULED, // not used
        IN_ADD, // added but not used
        SCHEDULED, // runs normally
        IN_REMOVE, // trying to remove
    }

    class DispatcherHandler implements EventHandler<EventEntry> {

        public long idMask;             // bitmask which actors to process

        long timeCounters[] = new long[64]; // holds profiling data
        long load = 0;                      // acummulated load of handler

        int profileTick = 0;                // counter when to profile again
        int eventTick = 0;                  // ticker incremented for each event happening
        int scheduleTick = 0;               // counter when to check actor rescheduling
        int emptyCount = 0;
        int fullCount = 0;
        int num;
        private DispacherState state = DispacherState.UNSCHEDULED;
        private int threadScheduleTick;

        DispatcherHandler(int num) {
            this.num = num;
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
                handleInAdd();
            }

            // check for movement
            final long actorsToMove = ev.actorsToMove;
            if ( actorsToMove != 0 ) {
                handleMove(ev, actorsToMove);
                return;
            }

            ev.debugSeen = true;
            if ( ev.done )
                return;
            eventTick++;


            // do processing (event id must match idMask or transitionalIdMask

            if ( ((idMask & ev.id) != 0 && ! ev.done) ) {
                // standard processing
                ev.done = true;
                processEvent(ev);
            }

            if (state == DispacherState.IN_REMOVE ) {
                if ( handleInRemove() )
                    return;
            }

            if ( eventTick > EVENTTICK_RESET) {
                load = 4 * load / 5; // fadeout
                eventTick = 0;
            }
            if ( isSchedulingHandler() ) {
                threadScheduleTick++;
                if ( threadScheduleTick > 10000 ) {
                    handleThreadScheduling();
                }
            }
        }

        private void handleThreadScheduling() {
            threadScheduleTick = 0;
            // if rebalancing does not help (N rounds of rebalance did not fix q)
            // and queue keeps growing => try get another thread
            if ( dispatcherIndex < MAX_THREADS && !ringBuffer.hasAvailableCapacity(ringBuffer.getBufferSize() / 2)) {
                emptyCount = 0;
                fullCount++;
                if ( fullCount > FULL_QUEUE_TICK_THRESH_FOR_ADD_THREAD ) {
                    scheduleDispatcher(false);
                    fullCount = 0;
                }
            } else {
                fullCount = 0;
                if ( ringBuffer.hasAvailableCapacity(ringBuffer.getBufferSize()*2/3) ) {
                    emptyCount++;
                    if (state != DispacherState.IN_ADD) {
                        if (emptyCount > EMPTY_QUEUE_TICK_THRESH_FOR_REMOVE_THREAD) {
                            triggerRemove();
                            emptyCount = 0;
                        }
                    }
                }
            }
        }

        private void handleMove(EventEntry ev, long actorsToMove) {
            final int targetNum = ev.targetNum;
            if ( targetNum == num ) {
                if ( BALANCE_DEBUG )
                    System.out.println("moved actor "+actorsToMove+" to "+num);
                if ( state == DispacherState.IN_REMOVE ) {
                    System.out.println("got assignement in_remove "+state);
                }
                idMask |= actorsToMove;
            } else if ( targetNum >= 0 ) {
                idMask &= ~actorsToMove;
                timeCounters[targetNum] = 0;
            } else {
                System.out.println("error targetNum="+targetNum);
                System.exit(1);
            }
            ev.done = true;
            return;
        }

        boolean isSchedulingHandler() {
            return num == dispatcherIndex-1;
        }

        private void handleInAdd() {
            dispatcherIndex++;
            state = DispacherState.SCHEDULED;
            scheduleActors(dispatcherIndex);
            System.out.println("scheduled dispatcher " + (dispatcherIndex - 1));
        }

        private void processEvent(EventEntry ev) {
            profileTick++;
            if ( profileTick > PROFILE_THRESH) {
                profileTick = 0;
                profiledRun(ev);
                scheduleTick++;
                if ( scheduleTick > REBALANCE_PER_PROFILETICK) {
                    if ( !ringBuffer.hasAvailableCapacity(ringBuffer.getBufferSize()*2/3) ) {
                        if ( handlers[dispatcherIndex-1].state == DispacherState.IN_REMOVE ) {
                            scheduleActors(dispatcherIndex-1);
                        } else
                            scheduleActors(dispatcherIndex);
                    }
                    scheduleTick = 0;
                }
            } else {
                ev.work();
            }
        }

        private boolean handleInRemove() {
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
                // state = DispacherState.SCHEDULED;
                //System.out.println("remove impossible, has new assignments " + num + " " + Long.toBinaryString(idMask) );
            } else {
                if ( isSchedulingHandler() ) {
                    removeHandler();
                    return true;
                } else {
                    System.out.println("unexpected state");
                    System.exit(-1);
                }
            }
            return false;
        }

        private void removeHandler() {
            dispatcherIndex--;
            System.out.println("removed " + num);
            state = DispacherState.UNSCHEDULED;
            disruptor.removeHandler(this);
        }

        private void profiledRun(EventEntry ev) {
            // profile
            long tim = System.nanoTime();
            ev.work();
            long dur = System.nanoTime() - tim;
            load += dur;
            timeCounters[ev.nr] = (dur+timeCounters[ev.nr])/2;
        }

        private void triggerRemove()
        {
            if ( dispatcherIndex > 1 ) {
                System.out.println("try remove " + num + " " +Long.toBinaryString(idMask) );
                state = DispacherState.IN_REMOVE;
                scheduleActors(dispatcherIndex - 1);
            }
        }

        private void scheduleActors(int maxDispatcher) {
            if ( dispatcherIndex <= 1 )
                return;

            long mask = 0;
            for (int i = 0; i < dispatcherIndex; i++) {
                DispatcherHandler handler = handlers[i];
                if ( (mask&handler.idMask & mask) != 0 ) {
                    System.out.println("FATAL ERROR ");
                    System.exit(-1);
                }
                mask |= mask&handler.idMask;
            }

            long cumCounters[][] = new long[64][]; // 0 = counter [1] = actNum
            for (int i = 0; i < cumCounters.length; i++) {
                cumCounters[i] = new long[2];
                cumCounters[i][1] = i;
            }
            for (int i = 0; i < dispatcherIndex; i++) {
                DispatcherHandler handler = handlers[i];
                for (int j = 0; j < handler.timeCounters.length; j++) {
                    cumCounters[j][0] += handler.timeCounters[j];
                }
            }
            Arrays.sort(cumCounters, (a,b) -> a[0]>b[0]?1:-1 );

            // FIXME: use accumulated load per thread to get better load
            // distribution of outlier-actors
            long newIdMasks[] = new long[maxDispatcher];
            int roundRobinIdx = 0;
            for (int i = 0; i < cumCounters.length; i++) {
                long[] cumCounter = cumCounters[i];
                newIdMasks[roundRobinIdx] |= 1l<<cumCounter[1];
                roundRobinIdx = (roundRobinIdx+1)% maxDispatcher;
            }

            long accum = 0;
            for (int i = 0; i < newIdMasks.length; i++) {
                long newIdMask = newIdMasks[i];
                movActor(newIdMask,handlers[i]);
                accum |= newIdMask;
            }
            if ( accum != -1 ) {
                System.out.println("?? accum "+Long.toBinaryString(accum));
            }
        }

        public void movActor(long actors2Move, DispatcherHandler newHandler) {
            if ( actors2Move == 0 ){
                System.out.println("oh noes");
                System.exit(0);
            }
            if (BALANCE_DEBUG)
                System.out.println("try move "+actors2Move+" to "+newHandler.num);
            disruptor.feedBackQueue.execute(() -> publishEvent(0,0, actors2Move, newHandler.num) );
        }

        private void dumpWorkers() {
            for (int i = 0; i < handlers.length; i++) {
                DispatcherHandler handler = handlers[i];
                System.out.println(handler);
            }
        }

    }

    DispatcherHandler handlers[];
    int dispatcherIndex = 1;
    RingBuffer<EventEntry> ringBuffer;

    void initDisruptor() {
        executor = (ThreadPoolExecutor) Executors.newCachedThreadPool();
        disruptor = new DynamicDisruptor( () -> new EventEntry(), 1024*128); // requires 4 MB l3 cache
        handlers = new DispatcherHandler[MAX_THREADS];
        for (int i = 0; i < handlers.length; i++) {
            handlers[i] = new DispatcherHandler(i);
        }
        handlers[0].idMask = 0xffffffffffffffffl;

        ringBuffer = disruptor.getRingBuffer();
        CountDownLatch latch = new CountDownLatch(1);
        for (int i = 0; i < 1; i++) {
            final int finalI = i;
            executor.execute(() -> disruptor.addHandler(handlers[finalI],latch));
            handlers[finalI].state = DispacherState.SCHEDULED;
            handlers[finalI].num = finalI;
            dispatcherIndex = finalI+1;
        }
        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    void scheduleDispatcher(boolean latched) {
        final int di = dispatcherIndex;
        if ( di < handlers.length ) {
            System.out.println("try schedule dispatcher " + dispatcherIndex);
            if ( handlers[dispatcherIndex-1].state == DispacherState.IN_REMOVE ) {
                handlers[dispatcherIndex-1].state = DispacherState.SCHEDULED;
                System.out.println("reverted pending remove due to add");
            }
            if ( handlers[di].state == DispacherState.IN_ADD ) {
                System.out.println("schedule already underway "+di);
                return;
            }
            handlers[di].state = DispacherState.IN_ADD;
            handlers[di].emptyCount = 0;
            CountDownLatch latch = latched ? new CountDownLatch(1) : null;
            executor.execute( () -> disruptor.addHandler(handlers[di],latch) );
            if ( latched ) {
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                dispatcherIndex++;
                handlers[di].state = DispacherState.SCHEDULED;
                System.out.println("scheduled dispatcher " + (dispatcherIndex - 1));
            }
        }
    }

    public void publishEvent(int nr, long nanos, long actors2Move, int targetNum) {
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
        requestEntry.actorsToMove = actors2Move;
        requestEntry.targetNum = targetNum;
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
            sched.publishEvent(actorId, 250 * (10 + actorId * 10),0,-1); //*actorId
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
