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
package pt.haslab.htapbench.core;

import pt.haslab.htapbench.api.BenchmarkModule;
import pt.haslab.htapbench.api.Worker;
import pt.haslab.htapbench.densitity.Clock;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Implements the ClientBalacer which is responsible to decider if more OLAP streams should be added to system.
 * The decision is bounded to a configurable error margin.
 * We accept to degrade up to ERROR_MARGIN(%) of the OLTP targetTPS in trade to add another OLAP Stream.
 */
public class ClientBalancer implements Runnable{
    private static final org.apache.log4j.Logger LOG = org.apache.log4j.Logger.getLogger(ClientBalancer.class);
    
    private final BenchmarkModule benchmarkModule;
    protected final Connection conn;
    protected final WorkloadConfiguration wrkld;
    private Results results;
    private boolean verbose;
    private int intervalMonitor;
    
    List<Worker> workersOLTP = null;
    List<Worker> workersOLAP = null;
    List<WorkloadConfiguration> workConfs = null;
    List<Integer> TPMS = null;
    
    private int targetTPS;
    
    //sampling rate;
    private final int deltaT = 60;
    private final double kp = 0.4;
    private final double ki = 0.03;
    
    private double error_margin = 0.2;
    
    private int TPM = 0;
    int previous_TPS = 0;
    private int max_TPS = 0;
    private int projected_TPM = 0;
    private int nclients = 0;
    private double error=0;
    private double integral =0;
    protected int olapStreams = 0;
    private boolean saturated = false;
    private boolean terminate =false;
    
    private Clock clock = null;
    
    LatencyRecord latencies;
    WorkloadState wrkldState;
    ArrayList<LatencyRecord.Sample> samples = new ArrayList<LatencyRecord.Sample>();
    
    // Interval requests used by the monitor
    private AtomicInteger intervalRequests = new AtomicInteger(0);

    public ClientBalancer(BenchmarkModule benchModule, List<Worker> workersOLTP, List<Worker> workersOLAP, WorkloadConfiguration wrkld, Clock clock, boolean verbose,int intervalMonitor,double error_margin){
        this.benchmarkModule=benchModule;
        this.wrkld=wrkld;
        this.verbose=verbose;
        this.intervalMonitor=intervalMonitor;
        this.targetTPS = benchModule.getWorkloadConfiguration().getTargetTPS();
        this.nclients = benchModule.getWorkloadConfiguration().getTerminals();
        this.projected_TPM = this.targetTPS * this.deltaT;
        
        
        this.workersOLTP = workersOLTP;
        this.workersOLAP = workersOLAP;
        this.workConfs = new ArrayList<WorkloadConfiguration>();
        this.TPMS = new ArrayList<Integer>();
        this.workConfs.add(wrkld);
        this.clock=clock;
        this.wrkldState=wrkld.getWorkloadState();    
        this.latencies = new LatencyRecord(System.nanoTime());
        
        this.error_margin=error_margin;
        
        try {
            this.conn = this.benchmarkModule.makeConnection();
            //if(benchModule.getWorkloadConfiguration().getDBType().equals(DatabaseType.MONETDB)){
                this.conn.setAutoCommit(false);
            //}
            this.conn.setTransactionIsolation(this.wrkld.getIsolationMode());
        } catch (SQLException ex) {
            throw new RuntimeException("Failed to connect to database", ex);
        }
    }
    
    /**
     * Computes the current TPM count by issuing a SQL statement to the database.
     * @throws SQLException 
     */
    private void getTPS_statement() throws SQLException{
        //PostGreSQL
        ResultSet rs = executeSQLStmt("select xact_commit from pg_stat_database");
        while (rs.next()) {
            int currentTPS = rs.getInt(1);
            if(rs.isLast()){
                currentTPS = rs.getInt(1);
                this.TPM = (currentTPS - previous_TPS);
                previous_TPS = currentTPS;
            }     
        }
    }
    
    /**
     * Executes a given SQL Statement.
     * @param stmt_text
     * @return the corresponding Result Set.
     * @throws SQLException 
     */
    private ResultSet executeSQLStmt(String stmt_text) throws SQLException{
        PreparedStatement stmt = conn.prepareStatement(stmt_text);
        
        ResultSet rs = null;
        try {
            rs = stmt.executeQuery();
        } catch(SQLException ex) {
            // If the system thinks we're missing a prepared statement, then we
            // should regenerate them.
            if (ex.getErrorCode() == 0 && ex.getSQLState() != null && ex.getSQLState().equals("07003")){
                rs = stmt.executeQuery();
            }
            else {
                throw ex;
            }    
        }
        return rs;
    }
    
    /**
     * Executes a given SQL Statement.
     * @param stmt_text
     * @return the corresponding Result Set.
     * @throws SQLException 
     */
    private void executeSQLUpdate(String stmt_text) throws SQLException{
        PreparedStatement stmt = conn.prepareStatement(stmt_text);
        
        try {
            stmt.executeUpdate();
        } catch(SQLException ex) {
            // If the system thinks we're missing a prepared statement, then we
            // should regenerate them.
            if (ex.getErrorCode() == 0 && ex.getSQLState() != null && ex.getSQLState().equals("07003")){
                stmt.executeUpdate();
            }
            else {
                throw ex;
            }    
        }
    }
    
    
    
    /**
     * Computes the current TPM count by reading all workers stats.
     */
    private void getTPM() throws SQLException{
        int txn_count = 0;
        for(Worker w : workersOLTP){
            txn_count = txn_count + w.getTxncount();
        }
        this.TPM = txn_count;
        this.TPMS.add(this.TPM);
    }
    
    @Override
    public void run(){        
        try {
            long measureStart= System.nanoTime();
            int power=0;
            int requests=0;
            this.latencies.addLatency(workersOLAP.size(), 0, 0, this.TPM, 0);
            intervalRequests.incrementAndGet();
             
            while(!terminate){
                requests++;
                Thread.sleep(deltaT*1000);
                //Reads and computes the current observed TPM.
                getTPM();
                
                this.error = this.projected_TPM - this.TPM;
                this.integral = this.integral + this.error/this.deltaT;
                
                double output = this.kp*this.error + this.ki*this.integral;
                
                /**
                 * Take decision.
                 * If the the total targetTPS is within the error margin --> Launch another OLAP Stream.
                 */
                LOG.info("TPM: "+this.TPM);
                LOG.info("output: "+ output);
                LOG.info("error: "+this.error);
                
                if(this.olapStreams == 0 || (!saturated  && output < this.error_margin*this.projected_TPM)){   
                    this.olapStreams++;
                   
                    this.workersOLAP.addAll(benchmarkModule.makeOLAPWorker(verbose,clock));
                    LOG.info("ClientBalancer: Going to lauch 1 OLAP stream. Total OLAP STreams: "+ workersOLAP.size());
                }
                else{
                    saturated=true;
                    LOG.info("***************************************************************************************************");
                    LOG.info("         ClientBalancer: The system is saturated. No more OLAP streams will be lauched.");
                    LOG.info("***************************************************************************************************");
                }
               
                LOG.info("***************************************************************************************************");
                LOG.info("                          #ACTIVE OLAP STREAMS: "+workersOLAP.size());
                LOG.info("***************************************************************************************************");
                
                long end = System.nanoTime();
                this.latencies.addLatency(workersOLAP.size(), 0 , 0, this.TPM, 0);
                intervalRequests.incrementAndGet();
                    
            }
            if(terminate){
                long measureEnd = System.nanoTime();

                for (LatencyRecord.Sample sample : getLatencyRecords()) {
                    if(sample!=null)
                        samples.add(sample);
                }
                Collections.sort(samples);

                // Compute stats on all the latencies
                int[] latencies = new int[samples.size()];
                for (int i = 0; i < samples.size(); ++i) {
                    latencies[i] = samples.get(i).latencyUs;
                }
                DistributionStatistics stats = DistributionStatistics.computeStatistics(latencies);

                results = new Results(measureEnd - measureStart, requests, stats, samples);
                results.setName("CLIENT BALANCER");
            } 
        } catch (InterruptedException ex) {
            Logger.getLogger(ClientBalancer.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(ClientBalancer.class.getName()).log(Level.SEVERE, null, ex);
        } catch (SQLException ex) {
            Logger.getLogger(ClientBalancer.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    public Results getResults(){
        return results;
    }
    
    public void terminate(){
        this.terminate=true;
    }
    
    public final Iterable<LatencyRecord.Sample> getLatencyRecords() {
        return this.latencies;
    }
    
    public final int getRequests() {
        return latencies.size();
    }
    
    private int computeTPM_Accelaration(List<Integer> measurements, int cycles, int delta){
        int time_delta = cycles * delta;
        int aux = 0 ;
        double tpm_delta = 0;
        
        for(Integer m : this.TPMS){
            aux = aux +m;
        }
        tpm_delta = aux / time_delta;
        return ((Double)tpm_delta).intValue();
    }
    
    private void terminateOLAPWorker(int ammount){
        for(int j=0; j<ammount;j++){
            try {
                this.workersOLAP.get(0).cancelStatement();
                this.workersOLAP.get(0).getConnection().close();
                this.workersOLAP.remove(0);
                
            } catch (SQLException ex) {
                Logger.getLogger(ClientBalancer.class.getName()).log(Level.SEVERE, null, ex);
                LOG.debug("Error whiel closing OLAP worker");
            }
        }
    }
}
