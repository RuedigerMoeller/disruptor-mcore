package de.ruedigermoeller.disruptorbench;

import de.ruedigermoeller.serialization.FSTConfiguration;
import de.ruedigermoeller.serialization.FSTObjectOutput;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

/**
 * Created by ruedi on 20.04.14.
 * 
 * Class which feeds a series of requests into a Service. The service than has to call back upon completion of a request.
 * There are 2 service implementations:
 * 1 DisruptorService scheduling N threads for decoding an M threads for encoding. Only one thread does actual processing, so
 *   'business logic' is singlethreaded (no locking required).
 * 2 ThreadPool based Service scheduling N threads. Each thread does all 3 steps (decode, process, encode) at the price of having to
 *   synchronize in when accessing shared data in business logic
 */
public class LoadFeeder {

    private static final boolean BIGMSG = false;

    public static class Request implements Serializable {

        protected String dummy;
        protected int data;
        HashMap encodingWork; // give en/decoding something todo in case

        public Request(String dummy, int data) {
            this.dummy = dummy;
            this.data = data;
            if ( BIGMSG ) {
                encodingWork = new HashMap();
                for (int i = 0; i < 10; i++)
                    encodingWork.put(i, "hallo " + i);
            }
        }
    }

    public static class Response extends Request {

        public Response(String dummy, int data) {
            super(dummy,data);
        }

    }

    byte[][] requests; // requests are prealloc'ed

    public LoadFeeder(int numberOfPreallocedRequests) {
        try {
            setup(numberOfPreallocedRequests);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }


    public void setup( int numberOfRequests ) throws IOException, ClassNotFoundException {
        requests = new byte[numberOfRequests][];
        for (int i = 0; i < requests.length; i++) {
            final FSTObjectOutput objectOutput = FSTConfiguration.getDefaultConfiguration().getObjectOutput();
            objectOutput.writeObject(new Request("hello"+i,i), Request.class);
            requests[i] = objectOutput.getCopyOfWrittenBuffer();
            objectOutput.flush();
        }
    }

    AtomicInteger respCount = new AtomicInteger(0);
    public void response(byte[] result) {
        respCount.incrementAndGet();
    } // count responses to trigger test end

    public long run(Service service, int num2Send) {
        long tim = System.currentTimeMillis();
        int reqIndex = 0;
        for ( int i=0; i < num2Send; i++ ) {
            service.processRequest(requests[reqIndex++]);
            if ( reqIndex == requests.length )
                reqIndex = 0;
        }
        while (respCount.get()<num2Send) // don't care. result is in the 100's of milliseconds
            LockSupport.parkNanos(10000);
        long res = System.currentTimeMillis() - tim;
        service.shutdown();
        return res;
    }

}
