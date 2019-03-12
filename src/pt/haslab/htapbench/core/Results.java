
/******************************************************************************
 *  Copyright 2015 by OLTPBenchmark Project                                   *
 *                                                                            *
 *  Licensed under the Apache License, Version 2.0 (the "License");           *
 *  you may not use this file except in compliance with the License.          *
 *  You may obtain a copy of the License at                                   *
 *                                                                            *
 *    http://www.apache.org/licenses/LICENSE-2.0                              *
 *                                                                            *
 *  Unless required by applicable law or agreed to in writing, software       *
 *  distributed under the License is distributed on an "AS IS" BASIS,         *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  *
 *  See the License for the specific language governing permissions and       *
 *  limitations under the License.                                            *
 ******************************************************************************
/*
 * Copyright 2017 by INESC TEC                                                                                                
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
package pt.haslab.htapbench.core;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import pt.haslab.htapbench.core.LatencyRecord.Sample;
import pt.haslab.htapbench.api.TransactionType;
import pt.haslab.htapbench.util.Histogram;

public final class Results {
    public final long nanoSeconds;
    public long nanoSecondsH;
    public final int measuredRequests;
    public final DistributionStatistics latencyDistribution;
    final Histogram<TransactionType> txnSuccess = new Histogram<TransactionType>(true);
    final Histogram<TransactionType> txnAbort = new Histogram<TransactionType>(true);
    final Histogram<TransactionType> txnRetry = new Histogram<TransactionType>(true);
    final Histogram<TransactionType> txnErrors = new Histogram<TransactionType>(true);
    final Map<TransactionType, Histogram<String>> txnAbortMessages = new HashMap<TransactionType, Histogram<String>>();
    private int ts_counter = 0;
    private String name;
    
    public final List<LatencyRecord.Sample> latencySamples;

    public Results(long nanoSeconds, int measuredRequests, DistributionStatistics latencyDistribution, final List<LatencyRecord.Sample> latencySamples) {
        this.nanoSeconds = nanoSeconds;
        this.measuredRequests = measuredRequests;
        this.latencyDistribution = latencyDistribution;

        if (latencyDistribution == null) {
            assert latencySamples == null;
            this.latencySamples = null;
        } else {
            // defensive copy
            this.latencySamples = Collections.unmodifiableList(new ArrayList<LatencyRecord.Sample>(latencySamples));
            assert !this.latencySamples.isEmpty();
        }
    }

    /**
     * Get a histogram of how often each transaction was executed
     */
    public final Histogram<TransactionType> getTransactionSuccessHistogram() {
        return (this.txnSuccess);
    }
    public final Histogram<TransactionType> getTransactionRetryHistogram() {
        return (this.txnRetry);
    }
    public final Histogram<TransactionType> getTransactionAbortHistogram() {
        return (this.txnAbort);
    }
    public final Histogram<TransactionType> getTransactionErrorHistogram() {
        return (this.txnErrors);
    }
    public final Map<TransactionType, Histogram<String>> getTransactionAbortMessageHistogram() {
        return (this.txnAbortMessages);
    }
    
    public void setName(String name){
        this.name=name;
    }
    
    public String getName(){
        return this.name;
    }

    public double getRequestsPerSecond() {
        return (double) measuredRequests / (double) nanoSeconds * 1e9;
    }
    
    public double get_QphH(){
        return (double)(measuredRequests)/ ((double)(nanoSeconds * 0.000000001)/360);
    }
    
    public double get_tpmC(){
        this.nanoSecondsH=nanoSeconds;
        return (double)(measuredRequests/ ((double)(nanoSeconds * 0.000000001) / 60))*0.45;
    }

    @Override
    public String toString() {
        String results ="Results(nanoSeconds=" + nanoSeconds + ", measuredRequests=" + measuredRequests + ") = " + getRequestsPerSecond() + " requests/sec \n "+
               "************************* \n"+
               "Total TS Count: " + this.ts_counter + "\n";
        if(getName().equals("TPCC"))
            results = results + "tpmC : " + get_tpmC() + "\n *************************";
        if(getName().equals("TPCH"))
            results = results + "QphH : " + get_QphH() + "\n *************************";
        return results;       
    }

    public void writeCSV(int windowSizeSeconds, PrintStream out) {
        writeCSV(windowSizeSeconds, out, TransactionType.INVALID);
    }
    
    public void writeCSV(int windowSizeSeconds, PrintStream out, TransactionType txType) {
        out.println("**********************************************");
        out.println(this.getName()+ " Results:");
        out.println("**********************************************");
        out.println("time(sec), throughput(req/sec), avg_lat(ms), min_lat(ms), 25th_lat(ms), median_lat(ms), 75th_lat(ms), 90th_lat(ms), 95th_lat(ms), 99th_lat(ms), max_lat(ms), tp (req/s) scaled");
        int i = 0;
        for (DistributionStatistics s : new TimeBucketIterable(latencySamples, windowSizeSeconds, txType)) {
            final double MILLISECONDS_FACTOR = 1e3;
            out.printf("%d,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f\n", i * windowSizeSeconds, (double) s.getCount() / windowSizeSeconds, s.getAverage() / MILLISECONDS_FACTOR,
                    s.getMinimum() / MILLISECONDS_FACTOR, s.get25thPercentile() / MILLISECONDS_FACTOR, s.getMedian() / MILLISECONDS_FACTOR, s.get75thPercentile() / MILLISECONDS_FACTOR,
                    s.get90thPercentile() / MILLISECONDS_FACTOR, s.get95thPercentile() / MILLISECONDS_FACTOR, s.get99thPercentile() / MILLISECONDS_FACTOR, s.getMaximum() / MILLISECONDS_FACTOR,
                    MILLISECONDS_FACTOR / s.getAverage());
            i += 1;
        }
        out.println(toString());
    }

    public void writeAllCSV(PrintStream out) {
        long startNs = latencySamples.get(0).startNs;
        
        out.println("transaction type (index in config file), start time (microseconds),latency (microseconds)");
        for (Sample s : latencySamples) {
            long startUs = (s.startNs - startNs + 500) / 1000;
            out.println(s.tranType + "," + startUs + "," + s.latencyUs);
        }
    }

    public void writeAllCSVAbsoluteTiming(PrintStream out) {

        // This is needed because nanTime does not guarantee offset... we
        // ground it (and round it) to ms from 1970-01-01 like currentTime
        double x = ((double) System.nanoTime() / (double) 1000000000);
        double y = ((double) System.currentTimeMillis() / (double) 1000);
        double offset = x - y;

        // long startNs = latencySamples.get(0).startNs;
        if(this.getName().endsWith("TPCC"))
            out.println("transaction type (index in config file), start time (microseconds),latency (microseconds),worker id(start number), phase id(index in config file)");
        if(this.getName().equals("TPCH"))
            out.println("transaction type (index in config file), start time (microseconds),latency (microseconds),worker id(start number), Rows in ResultSet");
        if(this.getName().equals("CLIENT BALANCER"))
            out.println("transaction type (index in config file), start time (microseconds),latency (microseconds), Current TPS , phase id(index in config file)");
        for (Sample s : latencySamples) {
            double startUs = ((double) s.startNs / (double) 1000000000);
            out.println(s.tranType + "," + String.format("%10.6f", startUs - offset) + "," + s.latencyUs + "," + s.workerId + "," + s.phaseId);
        }
    }
    
    public void setTsCounter(int ts_counter){
        this.ts_counter=ts_counter;
    }
    
    public int setTsCounter(){
        return this.ts_counter;
    }
}