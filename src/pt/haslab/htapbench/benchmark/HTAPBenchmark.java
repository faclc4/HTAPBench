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

import static pt.haslab.htapbench.benchmark.jTPCCConfig.terminalPrefix;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;



import pt.haslab.htapbench.procedures.tpcc.NewOrder;
import pt.haslab.htapbench.procedures.tpch.Q1;
import pt.haslab.htapbench.densitity.Clock;
import pt.haslab.htapbench.densitity.DensityConsultant;
import pt.haslab.htapbench.util.SimpleSystemPrinter;
import java.util.concurrent.atomic.AtomicInteger;
import pt.haslab.htapbench.api.BenchmarkModule;
import pt.haslab.htapbench.api.Loader;
import pt.haslab.htapbench.api.Worker;
import pt.haslab.htapbench.core.WorkloadConfiguration;

public class HTAPBenchmark extends BenchmarkModule {
    private static final Logger LOG = Logger.getLogger(HTAPBenchmark.class);
    private AtomicInteger ts_counter = null;
    private DensityConsultant density = null;
    

	public HTAPBenchmark(WorkloadConfiguration workConf) {
		super("htapb", workConf, true);
                ts_counter = new AtomicInteger();
                int targetTPS = workConf.getTargetTPS();
                this.density = new DensityConsultant(targetTPS);
	}

	@Override
	protected Package getProcedurePackageImpl() {
		return (NewOrder.class.getPackage());
	}
        
        /**
         * Imports packages for OLTP and OLAP queries.
         * This is necessary once they live in different packages for clarity.
         * @param txnName
         * @return 
         */
        protected Package getProcedurePackageImpl(String txnName) {
            if(txnName.startsWith("Q"))
                return (Q1.class.getPackage());
            else
                return (NewOrder.class.getPackage());
	}

        /**
         * This method either creates terminals for the OLTP or the OLAP stream.
         * @param verbose
         * @param worker
         * @return
         * @throws IOException 
         */
	@Override
	protected List<Worker> makeWorkersImpl(boolean verbose, String workerType, Clock clock) throws IOException {
            jTPCCConfig.TERMINAL_MESSAGES = false;
            ArrayList<Worker> workers = new ArrayList<Worker>();

            if(workerType.equals("TPCC")){
                try {
                    List<TPCCWorker> terminals = createTerminals(clock);
                    workers.addAll(terminals);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            if(workerType.equals("TPCH")){
                try {
                    List<TPCHWorker> terminals = createTerminals(workerType,clock);
                    workers.addAll(terminals);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            
            return workers;
	}
        
        @Override
        /**
         * Returns one OLAP terminal.
         */
        protected List<Worker> makeOLAPWorkerImpl(boolean verbose, Clock clock) throws IOException {
            jTPCCConfig.TERMINAL_MESSAGES = false;
            ArrayList<Worker> workers = new ArrayList<Worker>();
            
            try {
                    List<TPCHWorker> terminals = this.createTerminals("TPCH",clock);
                    workers.addAll(terminals);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            return workers;
        }

	@Override
	protected Loader makeLoaderImpl(Connection conn, boolean calibrate, boolean generateFiles, String fileLocation) throws SQLException {
		return new HTAPBLoader(this, conn, calibrate, generateFiles, fileLocation, this.density);
	}

        /**
         * This methods creates OLTP terminals
         * @param clock
         * @return
         * @throws SQLException 
         */
	protected ArrayList<TPCCWorker> createTerminals(Clock clock) throws SQLException {

		TPCCWorker[] terminals = new TPCCWorker[workConf.getTerminals()];

		int numWarehouses = (int) workConf.getScaleFactor();//tpccConf.getNumWarehouses();
		int numTerminals = workConf.getTerminals();
		assert (numTerminals >= numWarehouses) :
		    String.format("Insufficient number of terminals '%d' [numWarehouses=%d]",
		                  numTerminals, numWarehouses);

		String[] terminalNames = new String[numTerminals];
		int warehouseOffset = Integer.getInteger("warehouseOffset", 1);
		assert warehouseOffset == 1;

		// We distribute terminals evenly across the warehouses
		// Eg. if there are 10 terminals across 7 warehouses, they
		// are distributed as
		// 1, 1, 2, 1, 2, 1, 2
		final double terminalsPerWarehouse = (double) numTerminals
				/ numWarehouses;
		assert terminalsPerWarehouse >= 1;
		for (int w = 0; w < numWarehouses; w++) {
			// Compute the number of terminals in *this* warehouse
			int lowerTerminalId = (int) (w * terminalsPerWarehouse);
			int upperTerminalId = (int) ((w + 1) * terminalsPerWarehouse);
			// protect against double rounding errors
			int w_id = w + 1;
			if (w_id == numWarehouses)
				upperTerminalId = numTerminals;
			int numWarehouseTerminals = upperTerminalId - lowerTerminalId;

			LOG.info(String.format("w_id %d = %d terminals [lower=%d / upper%d]",
			                       w_id, numWarehouseTerminals, lowerTerminalId, upperTerminalId));

			final double districtsPerTerminal = jTPCCConfig.configDistPerWhse
					/ (double) numWarehouseTerminals;
			assert districtsPerTerminal >= 1 :
			    String.format("Too many terminals [districtsPerTerminal=%.2f, numWarehouseTerminals=%d]",
			                  districtsPerTerminal, numWarehouseTerminals);
			for (int terminalId = 0; terminalId < numWarehouseTerminals; terminalId++) {
				int lowerDistrictId = (int) (terminalId * districtsPerTerminal);
				int upperDistrictId = (int) ((terminalId + 1) * districtsPerTerminal);
				if (terminalId + 1 == numWarehouseTerminals) {
					upperDistrictId = jTPCCConfig.configDistPerWhse;
				}
				lowerDistrictId += 1;

				String terminalName = terminalPrefix + "w" + w_id + "d"
						+ lowerDistrictId + "-" + upperDistrictId;

				TPCCWorker terminal = new TPCCWorker(terminalName, w_id,
						lowerDistrictId, upperDistrictId, this,
						new SimpleSystemPrinter(null), new SimpleSystemPrinter(
								System.err), numWarehouses,ts_counter,clock);
				terminals[lowerTerminalId + terminalId] = terminal;
				terminalNames[lowerTerminalId + terminalId] = terminalName;
			}

		}
		assert terminals[terminals.length - 1] != null;

		ArrayList<TPCCWorker> ret = new ArrayList<TPCCWorker>();
		for (TPCCWorker w : terminals)
			ret.add(w);
		return ret;
	}

        /**
         * This method creates OLAP terminals
         * @return
         * @throws SQLException
         */
        protected ArrayList<TPCHWorker> createTerminals(String workerType,Clock clock) throws SQLException {
            int numTerminals = workConf.getOLAPTerminals();
            //int numTerminals = 1;

            ArrayList<TPCHWorker> ret = new ArrayList<TPCHWorker>();
            LOG.info(String.format("Creating %d workers for TPC-H", numTerminals));
            for (int i = 0; i < numTerminals; i++)
                ret.add(new TPCHWorker(this,clock));

            return ret;
    }
}
