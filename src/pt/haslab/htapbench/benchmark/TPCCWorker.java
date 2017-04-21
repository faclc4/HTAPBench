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
package pt.haslab.htapbench.benchmark;

/*
 * jTPCCTerminal - Terminal emulator code for jTPCC (transactions)
 *
 * Copyright (C) 2003, Raul Barbosa
 * Copyright (C) 2004-2006, Denis Lussier
 *
 */


import java.sql.SQLException;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import pt.haslab.htapbench.procedures.tpcc.TPCCProcedure;
import pt.haslab.htapbench.densitity.Clock;
import pt.haslab.htapbench.random.RandomParameters;
import pt.haslab.htapbench.types.ResultSetResult;
import pt.haslab.htapbench.types.TransactionStatus;
import pt.haslab.htapbench.util.SimplePrinter;
import pt.haslab.htapbench.api.Procedure.UserAbortException;
import pt.haslab.htapbench.api.TransactionType;
import pt.haslab.htapbench.api.Worker;
import pt.haslab.htapbench.core.WorkloadConfiguration;

public class TPCCWorker extends Worker {

	// private TransactionTypes transactionTypes;

	private String terminalName;

	private final int terminalWarehouseID;
	/** Forms a range [lower, upper] (inclusive). */
	private final int terminalDistrictLowerID;
	private final int terminalDistrictUpperID;
	private SimplePrinter terminalOutputArea, errorOutputArea;
	// private boolean debugMessages;
	private final Random rand = new Random();
        private final RandomParameters RandGen = new RandomParameters("uniform");     
	private int transactionCount = 1, numWarehouses;
	private static final AtomicInteger terminalId = new AtomicInteger(0);
        private AtomicInteger ts_counter = null;
        private Clock clock = null;
        
        public long thinkTime = 0;

	public TPCCWorker(String terminalName, int terminalWarehouseID,
			int terminalDistrictLowerID, int terminalDistrictUpperID,
			HTAPBenchmark benchmarkModule, SimplePrinter terminalOutputArea,
			SimplePrinter errorOutputArea, int numWarehouses, AtomicInteger ts_counter, Clock clock)
			throws SQLException {
		super(benchmarkModule, terminalId.getAndIncrement());
		
		this.terminalName = terminalName;

		this.terminalWarehouseID = terminalWarehouseID;
		this.terminalDistrictLowerID = terminalDistrictLowerID;
		this.terminalDistrictUpperID = terminalDistrictUpperID;
		assert this.terminalDistrictLowerID >= 1;
		assert this.terminalDistrictUpperID <= jTPCCConfig.configDistPerWhse;
		assert this.terminalDistrictLowerID <= this.terminalDistrictUpperID;
		this.terminalOutputArea = terminalOutputArea;
		this.errorOutputArea = errorOutputArea;
		this.numWarehouses = numWarehouses;
                this.ts_counter=ts_counter;
                this.clock = clock;
	}


	/**
	* Executes a single TPCC transaction of type transactionType.
        * @param nextTransaction
        * @param rows
        * @return 
        * @throws java.sql.SQLException
	 */
	@Override
    protected TransactionStatus executeWork(TransactionType nextTransaction, ResultSetResult rows) throws UserAbortException, SQLException {
        try {
            TPCCProcedure proc = (TPCCProcedure) this.getProcedure(nextTransaction.getProcedureClass());
            proc.run(conn, rand, terminalWarehouseID, numWarehouses,terminalDistrictLowerID, terminalDistrictUpperID, this);
            setThinkTime(thinkTime()+proc.getKeyingTime());
            
            //waits the required ThinkTime per txn.
            //Thread.sleep(getThinkTime());
            
            
        } catch (ClassCastException ex){
            //fail gracefully
        	System.err.println("TPC-C: We have been invoked with an INVALID transactionType?!");
        	throw new RuntimeException("Bad transaction type = "+ nextTransaction);
	    } catch (RuntimeException ex) {
	        conn.rollback();
                System.err.println("TPC-C txn will restart "+ex.getMessage());
	        return (TransactionStatus.RETRY_DIFFERENT);
	    }/* catch (InterruptedException ex) {
                Logger.getLogger(TPCCWorker.class.getName()).log(Level.SEVERE, null, ex);
            }*/
		transactionCount++;
        conn.commit();
        return (TransactionStatus.SUCCESS);
    }
    
    public AtomicInteger getTs_conter(){
        return this.ts_counter;
    }

    public WorkloadConfiguration getWrkld() {
        return wrkld;
    }
    
    public long thinkTime() {
        long r = RandGen.negExp(rand, getThinkTime(), 0.36788, getThinkTime(), 4.54e-5, getThinkTime());
        return (r);
    }
  
    private long getThinkTime(){
        return this.thinkTime;
    }
    
    private void setThinkTime(long thinkTime){
       this.thinkTime=thinkTime;
    }
    
    public Clock getClock(){
        return this.clock;
    }
    
}

