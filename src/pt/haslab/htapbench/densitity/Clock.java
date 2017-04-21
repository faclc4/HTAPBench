/*
 * Copyright 2017 by INESC TEC                                               
 * Developed by Fábio Coelho                                                 
 * This work was based on the OLTPBenchmark Project                          
 *
 * Licensed under the Apache License, Version 2.0 (the "License");           
 * you may not use this file except in compliance with the License.          
 * You may obtain a copy of the License at                                   
 *
 * http://www.apache.org/licenses/LICENSE-2.0                              
 *
 * Unless required by applicable law or agreed to in writing, software       
 * distributed under the License is distributed on an "AS IS" BASIS,         
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  
 * See the License for the specific language governing permissions and       
 * limitations under the License.                                            
 */
package pt.haslab.htapbench.densitity;

import java.sql.Timestamp;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;
import pt.haslab.htapbench.benchmark.jTPCCConfig;
import pt.haslab.htapbench.core.AuxiliarFileHandler;
import pt.haslab.htapbench.exceptions.HTAPBException;

/**
 * This class sets up the Clock considering the deltaTs found in density class.
 * The main purpose is to increment a global clock according to the deltaTs.
 * This will allow to reproduce in load time, the TS density found during TPC-C
 * execution for a given target TPS.
 */
public class Clock {

    private AtomicLong clock;
    private long deltaTs;
    private int warehouses;
    private long startTime;
    private long populateStartTs;

    private final int tpch_populate_slots = 2555;
    //TPC-H start date for equivalence: 1992-01-01 00:00:00
    //private final long tpch_start_date = 694224000000l;
    private final long tpch_start_date;

    public Clock(long deltaTS, boolean populate) {
        this.deltaTs = deltaTS;
        Timestamp tpch_start = new Timestamp(1992,1,1,0,0,0,0);
        this.tpch_start_date = tpch_start.getTime();
        
        if (populate) {
            this.startTime = System.currentTimeMillis();
            try {
                this.populateStartTs = getFinalPopulatedTs();
            } catch (HTAPBException ex) {
                Logger.getLogger(Clock.class.getName()).log(Level.SEVERE, null, ex);
            }
        } else {
            this.startTime = AuxiliarFileHandler.importLastTs("./");
            this.populateStartTs = AuxiliarFileHandler.importFirstTs("./");
        }
        this.clock = new AtomicLong(startTime);
        this.warehouses = 0;
    }

    public Clock(long deltaTS, int warehouses, boolean populate) {
        this.deltaTs = deltaTS;
        Timestamp tpch_start = new Timestamp(1992,1,1,0,0,0,0);
        this.tpch_start_date = tpch_start.getTime();
        if (populate) {
            this.startTime = System.currentTimeMillis();
            this.populateStartTs = startTime;
        } else {
            this.startTime = AuxiliarFileHandler.importLastTs("./");
            this.populateStartTs = AuxiliarFileHandler.importFirstTs("./");
        }
        this.clock = new AtomicLong(startTime);
        this.warehouses = warehouses;
    }

    /**
     * Increments the global clock and returns the new timestamp.
     *
     * @return
     */
    public long tick() {
        return clock.addAndGet(deltaTs);
    }

    /**
     * Returns the current value of the global clock.
     *
     * @return
     */
    public long getCurrentTs() {
        return clock.get();
    }

    /**
     * Returns the first generated Timestamp.
     *
     * @return
     */
    public long getStartTimestamp() {
        return this.startTime;
    }

    /**
     * Computes and returns the last populated TS.
     *
     * @return
     * @throws HTAPBException
     */
    public long getFinalPopulatedTs() throws HTAPBException {
        long res = 0;
        if (warehouses != 0) {
            long TSnumber = warehouses * jTPCCConfig.configDistPerWhse * jTPCCConfig.configCustPerDist * 2;
            System.out.println("# TS: " + TSnumber);
            res = startTime + deltaTs * TSnumber;
        } else {
            throw new HTAPBException("Number of warehouses undefined.");
        }

        return res;
    }

    /**
     * Computes the Timestamp to be used during dynamic query generation.
     *
     * @param ts : A target Timestamp
     * @return A new timestamp which is the offset between ts and the start Ts
     * in the TPC-H spec transformed to our dataset.
     */
    public long transformTsFromSpecToLong(long ts) {
        //number of timestamps;
        long tss = this.startTime - this.populateStartTs;
        //number of ts in each slot;
        long step = tss / this.tpch_populate_slots;
        
        long diff = ts - tpch_start_date;

        int diff_days = (int)((ts - tpch_start_date) / (24 * 60 * 60 * 1000));

        return this.populateStartTs + diff_days * step;
    }

    /**
     * Computes Timestamp + days.
     *
     * @param ts
     * @param days
     * @return
     */
    public long computeTsPlusXDays(long ts, int days) {
        long daysInLong = days * 24 * 60 * 60 * 1000;
        return ts + daysInLong;
    }
    
    /**
     * Computes Timestamp - days.
     * @param ts
     * @param days
     * @return 
     */
    public long computeTsMinusXDays(long ts, int days) {
        long daysInLong = days * 24 * 60 * 60 * 1000;
        return ts - daysInLong;
    }

    public long getPopulateStartTs() {
        return this.tpch_start_date;
    }

}
