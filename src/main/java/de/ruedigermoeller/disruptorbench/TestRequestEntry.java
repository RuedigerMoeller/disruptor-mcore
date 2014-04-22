package de.ruedigermoeller.disruptorbench;

import de.ruedigermoeller.serialization.FSTConfiguration;
import de.ruedigermoeller.serialization.FSTObjectInput;
import de.ruedigermoeller.serialization.FSTObjectOutput;

import java.io.OutputStream;
import java.util.Date;

/**
 * Created by ruedi on 21.04.14.
 * 
 * Helper during processing of an event. Consists of 3 methods encode(), decode(), and process()
 * 
*/
public class TestRequestEntry {

    static FSTConfiguration confRead = FSTConfiguration.createDefaultConfiguration();
    static FSTConfiguration confWrite = FSTConfiguration.createDefaultConfiguration();

    static {
        confRead.setShareReferences(false);
        confWrite.setShareReferences(false);
    }
    // used for partitioning encoding/decoding in disruptor
    public int decPartition;
    public int encPartition;

    byte [] rawRequest;
    LoadFeeder.Request req; // after decoding
    LoadFeeder.Response resp =  new LoadFeeder.Response(null,0);

    // can be multithreaded
    public void decode() throws Exception {
        final FSTObjectInput objectInput = confRead.getObjectInput(rawRequest);
        req = (LoadFeeder.Request) objectInput.readObject();
    }

    // single threaded or need synchronization in the SharedData implementation
    public void process(SharedData data) throws Exception {
        Integer result = data.lookup(req.data);
        if ( result == null )
            result = 0;
        // just mimic some simple business logic involving some alloc
        for (int i = 0; i < 20; i++) {
            Date d = new Date(result);
            result = (int) d.getTime();
        }
        resp.data = req.data;
    }

    // can be multithreaded
    public void encode(LoadFeeder serv) throws Exception {
        FSTObjectOutput out = confWrite.getObjectOutput((OutputStream) null);
        out.writeObject(resp);
        serv.response(out.getCopyOfWrittenBuffer());
        out.flush();
    }

}
