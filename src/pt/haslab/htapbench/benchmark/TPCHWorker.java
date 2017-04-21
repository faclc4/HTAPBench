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

import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;
import pt.haslab.htapbench.api.BenchmarkModule;
import pt.haslab.htapbench.api.Procedure;
import pt.haslab.htapbench.api.Procedure.UserAbortException;
import pt.haslab.htapbench.api.TransactionType;
import pt.haslab.htapbench.api.Worker;

import pt.haslab.htapbench.procedures.tpch.GenericQuery;
import pt.haslab.htapbench.densitity.Clock;
import pt.haslab.htapbench.types.ResultSetResult;
import pt.haslab.htapbench.types.TransactionStatus;

public class TPCHWorker extends Worker {
    
    private Clock clock = null;
    
    public TPCHWorker(BenchmarkModule benchmarkModule,Clock clock) {
        super(benchmarkModule, terminalId.getAndIncrement());
        this.clock=clock;
    }

    private static final AtomicInteger terminalId = new AtomicInteger(0);

    @Override
    protected TransactionStatus executeWork(TransactionType nextTransaction,ResultSetResult rows) throws UserAbortException, SQLException {
        try {
            Class<? extends Procedure> p = nextTransaction.getProcedureClass();            
            Procedure getProc = this.getProcedure(p);
            
            //GenericQuery proc = (GenericQuery) this.getProcedure(nextTransaction.getProcedureClass());
            GenericQuery proc = (GenericQuery) getProc;
            proc.setOwner(this);           
            int resultSetRowNumber = proc.run(conn,clock,super.getWorkloadConfiguration());
            rows.setRows(resultSetRowNumber);
        } catch (ClassCastException e) {
            System.err.println(e.toString());
            System.err.println("TPC-H : We have been invoked with an INVALID transactionType?!");
            throw new RuntimeException("Bad transaction type = "+ nextTransaction);
        }
        //TPCH transactions cannont be commited. If they are it will interfere with the statistics return.
        //conn.commit();
        return (TransactionStatus.SUCCESS);

    }
}

