package de.ruedigermoeller.disruptorbench;

import java.util.HashMap;

/**
 * Created by ruedi on 21.04.14.
 * 
 * Emulate access to shared data using unsynchronized HashMap. 
 * Requires single threaded business logic
 */
public class SingleThreadedSharedData implements SharedData {

    HashMap aMap;

    public SingleThreadedSharedData() {
        this.aMap = new HashMap();
        for ( int i = 0; i < 1000; i++ ) {
            aMap.put(i,i);
        }
    }

    @Override
    public Integer lookup(int i) {
        return (Integer) aMap.get(i);
    }
}
