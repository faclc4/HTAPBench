
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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.log4j.Logger;

import pt.haslab.htapbench.api.TransactionType;
import pt.haslab.htapbench.api.Worker;
import pt.haslab.htapbench.benchmark.TPCCWorker;
import pt.haslab.htapbench.util.Histogram;
import pt.haslab.htapbench.util.QueueLimitException;
import pt.haslab.htapbench.util.StringUtil;
import pt.haslab.htapbench.types.State;

public class ThreadBench implements Thread.UncaughtExceptionHandler{
    private static final Logger LOG = Logger.getLogger(ThreadBench.class);
    
    private static BenchmarkState oltptestState, olaptestState;
    private final List<? extends Worker> workers;
    private final ArrayList<Thread> workerThreads;
    // private File profileFile;
    private List<WorkloadConfiguration> workConfs;
    private List<WorkloadState> workStates;
    ArrayList<LatencyRecord.Sample> samples = new ArrayList<LatencyRecord.Sample>();
    private int intervalMonitor = 0;
    private boolean calibrate = false;
    private Results results;

    public ThreadBench(List<? extends Worker> workers, List<WorkloadConfiguration> workConfs, boolean calibrate, int intervalMonitor) {
        this(workers, null, workConfs,calibrate);
        this.intervalMonitor=intervalMonitor;
    }

    public ThreadBench(List<? extends Worker> workers, File profileFile, List<WorkloadConfiguration> workConfs, boolean calibrate) {
        this.workers = workers;
        this.workConfs = workConfs;
        this.workerThreads = new ArrayList<Thread>(workers.size());
        this.calibrate=calibrate;
    }

    private void createWorkerThreads() {
        for (Worker worker : workers) {
            worker.initializeState();
            Thread thread = new Thread(worker);
            thread.setUncaughtExceptionHandler(this);
            thread.start();
            this.workerThreads.add(thread);
        }
    }
    
    public Results getResults(){
        return this.results;
    }

    private void interruptWorkers() {
        for (Worker worker : workers) {
            LOG.info("Worker: [" +worker.getId()+"] is canceling statement");
            worker.cancelStatement();
        }
        
    }

    private int finalizeWorkers(ArrayList<Thread> workerThreads, BenchmarkState testState) throws InterruptedException {
        assert testState.getState() == State.DONE || testState.getState() == State.EXIT;
        int requests = 0;

        new WatchDogThread(testState).start();

        for (int i = 0; i < workerThreads.size(); ++i) {
            LOG.debug("Worker: ["+workerThreads.get(i).getId()+"] is finalizing");
            workerThreads.get(i).join(30000);
            //if(workerThreads.get(i).isAlive()){
            //    workerThreads.get(i).interrupt();
            //}
            requests += workers.get(i).getRequests();
            workers.get(i).tearDown(false);
        }
        testState = null;
        return requests;
    }

    private class WatchDogThread extends Thread {
        private BenchmarkState testState;
        
        public WatchDogThread(BenchmarkState testState)
        {
            this.setDaemon(true);
            this.testState=testState;
        }

        @Override
        public void run() {
            Map<String, Object> m = new ListOrderedMap<String, Object>();
            LOG.info("Starting WatchDogThread");
            while (true) {
                try {
                    Thread.sleep(20000);
                } catch (InterruptedException ex) {
                    return;
                }
                if (testState == null)
                    return;
                m.clear();
                for (Thread t : workerThreads) {
                    m.put(t.getName(), t.isAlive());
                }
                LOG.debug("Worker Thread Status:\n" + StringUtil.formatMaps(m));
            } // WHILE
        }
    } // CLASS

    private class MonitorThread extends Thread {
        private final int intervalMonitor;
        private BenchmarkState testState;
        
        MonitorThread(int interval, BenchmarkState testState) {
            this.setDaemon(true);
            this.intervalMonitor = interval;
            this.testState = testState;
        }
        @Override
        public void run() {
            LOG.info("Starting MonitorThread Interval[" + this.intervalMonitor + " seconds]");
            while (true) {
                try {
                    Thread.sleep(this.intervalMonitor * 1000);
                } catch (InterruptedException ex) {
                    return;
                }
                if (testState == null)
                    return;
                // Compute the last throughput
                long measuredRequests = 0;
                synchronized (testState) {
                    for (Worker w : workers) {
                        measuredRequests += w.getAndResetIntervalRequests();
                    }
                }
                double tps = (double) measuredRequests / (double) this.intervalMonitor;
                LOG.info("Throughput: " + tps + " Tps");
            } // WHILE
        }
    } // CLASS

    public static Results runRateLimitedOLTP(List<Worker> workers, List<WorkloadConfiguration> workConfs, int intervalMonitoring, boolean calibrate) throws QueueLimitException, IOException {
        ThreadBench bench = new ThreadBench(workers, workConfs,calibrate,intervalMonitoring);
        bench.intervalMonitor = intervalMonitoring;
        return bench.runRateLimitedMultiPhase();
    }
    
    public static Results runOLAP(List<Worker> workers, List<WorkloadConfiguration> workConfs, int intervalMonitoring, boolean calibrate) throws QueueLimitException, IOException {
        ThreadBench bench = new ThreadBench(workers, workConfs,calibrate,intervalMonitoring);
        bench.intervalMonitor = intervalMonitoring;
        return bench.runRateLimitedMultiPhase("OLAP");
    }   
    
    public Results runRateLimitedMultiPhase(String name) throws QueueLimitException, IOException {
        assert olaptestState == null;
        olaptestState = new BenchmarkState(workers.size() + 1);
        workStates = new ArrayList<WorkloadState>();

        for (WorkloadConfiguration workState : this.workConfs) {
            workStates.add(workState.initializeState(olaptestState));
        }

        this.createWorkerThreads();
        olaptestState.blockForStart();

        // long measureStart = start;

        long start = System.nanoTime();
        long start2 = start;
        long measureEnd = -1;
        // used to determine the longest sleep interval
        int lowestRate = Integer.MAX_VALUE;

        Phase phase = null;

        for (WorkloadState workState : this.workStates) {
            workState.switchToNextPhase();
            phase = workState.getCurrentPhase();
            LOG.info(phase.currentPhaseString());
            if (phase.rate < lowestRate) {
                lowestRate = phase.rate;
            }
        }

        long intervalNs = getInterval(lowestRate, phase.arrival);

        long nextInterval = start + intervalNs;
        int nextToAdd = 1;
        int rateFactor;

        boolean resetQueues = true;

        long delta = phase.time * 1000000000L;
        boolean lastEntry = false;

        // Initialize the Monitor
        if(this.intervalMonitor > 0 ) {
            new MonitorThread(this.intervalMonitor,olaptestState).start();
        }

        // Main Loop
        while (true) {           
            // posting new work... and reseting the queue in case we have new
            // portion of the workload...

            for (WorkloadState workState : this.workStates) {
                if (workState.getCurrentPhase() != null) {
                    rateFactor = workState.getCurrentPhase().rate / lowestRate;
                } else {
                    rateFactor = 1;
                }
                workState.addToQueue(nextToAdd * rateFactor, resetQueues,workers.get(0));
            }
            resetQueues = false;

            // Wait until the interval expires, which may be "don't wait"
            long now = System.nanoTime();
            long diff = nextInterval - now;
            while (diff > 0) { // this can wake early: sleep multiple times to
                               // avoid that
                long ms = diff / 1000000;
                diff = diff % 1000000;
                try {
                    Thread.sleep(ms, (int) diff);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                now = System.nanoTime();
                diff = nextInterval - now;
            }
            assert diff <= 0;

            boolean phaseComplete = false;
            if (phase != null) {
                TraceReader tr = workConfs.get(0).getTraceReader();
                if (tr != null) {
                    // If a trace script is present, the phase complete iff the
                    // trace reader has no more 
                    for (WorkloadConfiguration workConf : workConfs) {
                        phaseComplete = false;
                        tr = workConf.getTraceReader();
                        assert workConf.getTraceReader() != null;
                        if (!workConf.getWorkloadState().getScriptPhaseComplete()) {
                            break;
                        }
                        phaseComplete = true;
                    }
                }
                else if (phase.isLatencyRun())
                    // Latency runs (serial run through each query) have their own
                    // state to mark completion
                    phaseComplete = olaptestState.getState()
                                    == State.LATENCY_COMPLETE;
                else
                    phaseComplete = olaptestState.getState() == State.MEASURE
                                    && (start + delta <= now);
            }

            // Go to next phase if this one is complete
            if (phaseComplete && !lastEntry) {
                // enters here after each phase of the test
                // reset the queues so that the new phase is not affected by the
                // queue of the previous one
                resetQueues = true;

                // Fetch a new Phase
                synchronized (olaptestState) {
                    if (phase.isLatencyRun()) {
                        olaptestState.ackLatencyComplete();
                    }
                    for (WorkloadState workState : workStates) {
                        synchronized (workState) {
                            workState.switchToNextPhase();
                            lowestRate = Integer.MAX_VALUE;
                            phase = workState.getCurrentPhase();
                            interruptWorkers();
                            
                            
                            if (phase == null && !lastEntry) {
                                // Last phase
                                lastEntry = true;
                                olaptestState.startCoolDown();
                                measureEnd = now;
                                LOG.info("[TPC_H Terminate] Waiting for all terminals to finish ..");
                            } else if (phase != null) {
                                phase.resetSerial();
                                LOG.info(phase.currentPhaseString());
                            if (phase.rate < lowestRate) {
                                lowestRate = phase.rate;
                            }
                        }
                    }
                    }
                    if (phase != null) {
                        // update frequency in which we check according to
                        // wakeup
                        // speed
                        // intervalNs = (long) (1000000000. / (double)
                        // lowestRate + 0.5);
                        delta += phase.time * 1000000000L;
                    }
                }
            }

            // Compute the next interval
            // and how many messages to deliver
            if (phase != null) {
                intervalNs = 0;
                nextToAdd = 0;
                do {
                    intervalNs += getInterval(lowestRate, phase.arrival);
                    nextToAdd++;
                } while ((-diff) > intervalNs && !lastEntry);
                nextInterval += intervalNs;
            }

            // Update the test state appropriately
            State state = olaptestState.getState();
            if (state == State.WARMUP && now >= start) {
                synchronized(olaptestState) {
                    if (phase != null && phase.isLatencyRun())
                        olaptestState.startColdQuery();
                    else
                olaptestState.startMeasure();
                    interruptWorkers();
                }
                start = now;
                LOG.info("[Measure] Warmup complete, starting measurements.");
                // measureEnd = measureStart + measureSeconds * 1000000000L;

                // For serial executions, we want to do every query exactly
                // once, so we need to restart in case some of the queries
                // began during the warmup phase.
                // If we're not doing serial executions, this function has no
                // effect and is thus safe to call regardless.
                phase.resetSerial();
            } else if (state == State.EXIT) {
                // All threads have noticed the done, meaning all measured
                // requests have definitely finished.
                // Time to quit.
                break;
            }
        }

        try {
            int requests = finalizeWorkers(this.workerThreads,olaptestState);

            // Combine all the latencies together in the most disgusting way
            // possible: sorting!
            for (Worker w : workers) {
                if (w.getLatencyRecords() != null){
                    LOG.info("Worker: ["+w.getId()+"] is finalizing");
                    for (LatencyRecord.Sample sample : w.getLatencyRecords()) {
                        if(sample!=null)
                            samples.add(sample);
                    }
                }
            }
            Collections.sort(samples);

            // Compute stats on all the latencies
            int[] latencies = new int[samples.size()];
            for (int i = 0; i < samples.size(); ++i) {
                latencies[i] = samples.get(i).latencyUs;
            }
            DistributionStatistics stats = DistributionStatistics.computeStatistics(latencies);

            Results results = new Results(measureEnd - start2, requests, stats, samples);
            results.setName("TPCH");
            // Compute transaction histogram
            Set<TransactionType> txnTypes = new HashSet<TransactionType>();
            for (WorkloadConfiguration workConf : workConfs) {
                txnTypes.addAll(workConf.getTransTypes());
            }
            txnTypes.remove(TransactionType.INVALID);

            results.txnSuccess.putAll(txnTypes, 0);
            results.txnRetry.putAll(txnTypes, 0);
            results.txnAbort.putAll(txnTypes, 0);
            results.txnErrors.putAll(txnTypes, 0);

            for (Worker w : workers) {
                results.txnSuccess.putHistogram(w.getTransactionSuccessHistogram());
                results.txnRetry.putHistogram(w.getTransactionRetryHistogram());
                results.txnAbort.putHistogram(w.getTransactionAbortHistogram());
                results.txnErrors.putHistogram(w.getTransactionErrorHistogram());
                
                
                if(w.getWorkloadConfiguration().getCalibrate()){
                    if(w instanceof TPCCWorker)
                        results.setTsCounter(((TPCCWorker)w).getTs_conter().get());
                }

                for (Entry<TransactionType, Histogram<String>> e : w.getTransactionAbortMessageHistogram().entrySet()) {
                    Histogram<String> h = results.txnAbortMessages.get(e.getKey());
                    if (h == null) {
                        h = new Histogram<String>(true);
                        results.txnAbortMessages.put(e.getKey(), h);
                    }
                    h.putHistogram(e.getValue());
                } // FOR
            } // FOR

            return (results);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public Results runRateLimitedMultiPhase() throws QueueLimitException, IOException {
        assert oltptestState == null;
        oltptestState = new BenchmarkState(workers.size() + 1);
        workStates = new ArrayList<WorkloadState>();

        for (WorkloadConfiguration workState : this.workConfs) {
            workStates.add(workState.initializeState(oltptestState));
        }

        this.createWorkerThreads();
        oltptestState.blockForStart();

        // long measureStart = start;

        long start = System.nanoTime();
        long measureEnd = -1;
        // used to determine the longest sleep interval
        int lowestRate = Integer.MAX_VALUE;

        Phase phase = null;

        for (WorkloadState workState : this.workStates) {
            workState.switchToNextPhase();
            phase = workState.getCurrentPhase();
            LOG.info(phase.currentPhaseString());
            if (phase.rate < lowestRate) {
                lowestRate = phase.rate;
            }
        }

        long intervalNs = getInterval(lowestRate, phase.arrival);

        long nextInterval = start + intervalNs;
        int nextToAdd = 1;
        int rateFactor;

        boolean resetQueues = true;

        long delta = phase.time * 1000000000L;
        boolean lastEntry = false;

        // Initialize the Monitor
        if(this.intervalMonitor > 0 ) {
            new MonitorThread(this.intervalMonitor,oltptestState).start();
        }

        // Main Loop
        boolean execute = true;
        while (execute) {           
            // posting new work... and reseting the queue in case we have new
            // portion of the workload...

            for (WorkloadState workState : this.workStates) {
                if (workState.getCurrentPhase() != null) {
                    rateFactor = workState.getCurrentPhase().rate / lowestRate;
                } else {
                    rateFactor = 1;
                }
                workState.addToQueue(nextToAdd * rateFactor, resetQueues,workers.get(0));
            }
            resetQueues = false;

            // Wait until the interval expires, which may be "don't wait"
            long now = System.nanoTime();
            long diff = nextInterval - now;
            while (diff > 0) { // this can wake early: sleep multiple times to
                               // avoid that
                long ms = diff / 1000000;
                diff = diff % 1000000;
                try {
                    Thread.sleep(ms, (int) diff);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                now = System.nanoTime();
                diff = nextInterval - now;
            }
            assert diff <= 0;

            boolean phaseComplete = false;
            if (phase != null) {
                TraceReader tr = workConfs.get(0).getTraceReader();
                if (tr != null) {
                    // If a trace script is present, the phase complete iff the
                    // trace reader has no more 
                    for (WorkloadConfiguration workConf : workConfs) {
                        phaseComplete = false;
                        tr = workConf.getTraceReader();
                        assert workConf.getTraceReader() != null;
                        if (!workConf.getWorkloadState().getScriptPhaseComplete()) {
                            break;
                        }
                        phaseComplete = true;
                    }
                }
                else if (phase.isLatencyRun())
                    // Latency runs (serial run through each query) have their own
                    // state to mark completion
                    phaseComplete = oltptestState.getState() == State.LATENCY_COMPLETE;
                else
                    //phaseComplete = testState.getState() == State.MEASURE && (start + delta <= now);
                    phaseComplete = (start + delta <= now);
            }

            // Go to next phase if this one is complete
            if (phaseComplete && !lastEntry) {
                // enters here after each phase of the test
                // reset the queues so that the new phase is not affected by the
                // queue of the previous one
                resetQueues = true;

                // Fetch a new Phase
                synchronized (oltptestState) {
                    if (phase.isLatencyRun()) {
                        oltptestState.ackLatencyComplete();
                    }
                    for (WorkloadState workState : workStates) {
                        synchronized (workState) {
                            workState.switchToNextPhase();
                            lowestRate = Integer.MAX_VALUE;
                            phase = workState.getCurrentPhase();
                            interruptWorkers();
                            if (phase == null && !lastEntry) {
                                // Last phase
                                lastEntry = true;
                                oltptestState.startCoolDown();
                                measureEnd = now;
                                LOG.info("[Terminate TPC-C] Waiting for all terminals to finish ..");
                                execute=false;
                            } else if (phase != null) {
                                phase.resetSerial();
                                LOG.info(phase.currentPhaseString());
                            if (phase.rate < lowestRate) {
                                lowestRate = phase.rate;
                            }
                        }
                    }
                    }
                    if (phase != null) {
                        // update frequency in which we check according to
                        // wakeup
                        // speed
                        // intervalNs = (long) (1000000000. / (double)
                        // lowestRate + 0.5);
                        delta += phase.time * 1000000000L;
                    }
                }
            }

            // Compute the next interval
            // and how many messages to deliver
            if (phase != null) {
                intervalNs = 0;
                nextToAdd = 0;
                do {
                    intervalNs += getInterval(lowestRate, phase.arrival);
                    nextToAdd++;
                } while ((-diff) > intervalNs && !lastEntry);
                nextInterval += intervalNs;
            }

            // Update the test state appropriately
            State state = oltptestState.getState();
            if (state == State.WARMUP && now >= start) {
                synchronized(oltptestState) {
                    if (phase != null && phase.isLatencyRun())
                        oltptestState.startColdQuery();
                    else
                    oltptestState.startMeasure();
                    interruptWorkers();
                }
                start = now;
                LOG.info("[Measure] Warmup complete, starting measurements.");
                // measureEnd = measureStart + measureSeconds * 1000000000L;

                // For serial executions, we want to do every query exactly
                // once, so we need to restart in case some of the queries
                // began during the warmup phase.
                // If we're not doing serial executions, this function has no
                // effect and is thus safe to call regardless.
                phase.resetSerial();
            } else if (state == State.EXIT) {
                // All threads have noticed the done, meaning all measured
                // requests have definitely finished.
                // Time to quit.
                break;
            }
        }

        try {
            int requests = finalizeWorkers(this.workerThreads,oltptestState);

            // Combine all the latencies together in the most disgusting way
            // possible: sorting!
            for (Worker w : workers) {
                for (LatencyRecord.Sample sample : w.getLatencyRecords()) {
                    samples.add(sample);
                }
            }
            Collections.sort(samples);

            // Compute stats on all the latencies
            int[] latencies = new int[samples.size()];
            for (int i = 0; i < samples.size(); ++i) {
                latencies[i] = samples.get(i).latencyUs;
            }
            DistributionStatistics stats = DistributionStatistics.computeStatistics(latencies);

            Results results = new Results(measureEnd - start, requests, stats, samples);
            results.setName("TPCC");
            // Compute transaction histogram
            Set<TransactionType> txnTypes = new HashSet<TransactionType>();
            for (WorkloadConfiguration workConf : workConfs) {
                txnTypes.addAll(workConf.getTransTypes());
            }
            txnTypes.remove(TransactionType.INVALID);

            results.txnSuccess.putAll(txnTypes, 0);
            results.txnRetry.putAll(txnTypes, 0);
            results.txnAbort.putAll(txnTypes, 0);
            results.txnErrors.putAll(txnTypes, 0);

            for (Worker w : workers) {
                results.txnSuccess.putHistogram(w.getTransactionSuccessHistogram());
                results.txnRetry.putHistogram(w.getTransactionRetryHistogram());
                results.txnAbort.putHistogram(w.getTransactionAbortHistogram());
                results.txnErrors.putHistogram(w.getTransactionErrorHistogram());
                
                
                if(w.getWorkloadConfiguration().getCalibrate()){
                    results.setTsCounter(((TPCCWorker)w).getTs_conter().get());
                }

                for (Entry<TransactionType, Histogram<String>> e : w.getTransactionAbortMessageHistogram().entrySet()) {
                    Histogram<String> h = results.txnAbortMessages.get(e.getKey());
                    if (h == null) {
                        h = new Histogram<String>(true);
                        results.txnAbortMessages.put(e.getKey(), h);
                    }
                    h.putHistogram(e.getValue());
                } // FOR
            } // FOR

            return (results);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private long getInterval(int lowestRate, Phase.Arrival arrival) {
        if (arrival == Phase.Arrival.POISSON)
            return (long) ((-Math.log(1 - Math.random()) / lowestRate) * 1000000000.);
        else
            return (long) (1000000000. / (double) lowestRate + 0.5);
    }

    public boolean isCalibrate(){
        return this.calibrate;
    }

    @Override
    public void uncaughtException(Thread t, Throwable e) {
    }

    
}
