
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

import java.lang.reflect.Field;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.commons.configuration.XMLConfiguration;

import pt.haslab.htapbench.api.TransactionTypes;
import pt.haslab.htapbench.types.DatabaseType;
import pt.haslab.htapbench.util.StringUtil;

public class WorkloadConfiguration {
    
	private DatabaseType db_type;	
	private String benchmarkName;
	public String getBenchmarkName() {
        return benchmarkName;
    }

    public void setBenchmarkName(String benchmarkName) {
        this.benchmarkName = benchmarkName;
    }

    private String db_connection;
    private String db_name;
    private String db_username;
    private String db_password;
    private String db_driver;	
    private double scaleFactor = 1.0;
    private int terminals;
    private int OLAPterminals;
    private int numTxnTypes;
    private boolean calibrate =false;
    private boolean generateFiles = false;
    private String filesPath = "";
    private TraceReader traceReader = null;
    private int targetTPS;
    
    public TraceReader getTraceReader() {
        return traceReader;
    }
    public void setTraceReader(TraceReader traceReader) {
        this.traceReader = traceReader;
    }
    
	private XMLConfiguration xmlConfig = null;

	private List<Phase> works = new ArrayList<Phase>();
	private WorkloadState workloadState;

	public WorkloadState getWorkloadState() {
        return workloadState;
    }
	
	/**
	 * Initiate a new benchmark and workload state
	 */
    public WorkloadState initializeState(BenchmarkState benchmarkState) {
        assert (workloadState == null);
        workloadState = new WorkloadState(benchmarkState, works, terminals, traceReader,calibrate);
        return workloadState;
    }

    private int numberOfPhases = 0;
	private TransactionTypes transTypes = null;
	private int isolationMode = Connection.TRANSACTION_SERIALIZABLE;
	private boolean recordAbortMessages = false;
    private String dataDir = null;

 

    public void addWork(int time, int rate, List<String> weights, boolean rateLimited, boolean disabled, boolean serial, boolean timed, int active_terminals, Phase.Arrival arrival) {
        works.add(new Phase(benchmarkName, numberOfPhases, time, rate, weights, rateLimited, disabled, serial, timed, active_terminals, arrival));
		numberOfPhases++;
	}
	
	public void setDBType(DatabaseType dbType) {
        db_type = dbType;
    }
	
	public DatabaseType getDBType() {
        return db_type;
    }
	
	public void setDBConnection(String database) {
		this.db_connection = database;
	}
	
	public String getDBConnection() {
		return db_connection;
	}
	
	public void setDBName(String dbname) {
		this.db_name = dbname;
	}
	
	public int getNumTxnTypes() {
		return numTxnTypes;
	}
	
	public void setNumTxnTypes(int numTxnTypes) {
		this.numTxnTypes = numTxnTypes;
	}
	
	public String getDBName() {
		return db_name;
	}

	public void setDBUsername(String username) {
		this.db_username = username;
	}
	
	public String getDBUsername() {
		return db_username;
	}

	public void setDBPassword(String password) {
		this.db_password = password;
	}
        
        public void setTargetTPS(int targetTPS){
            this.targetTPS=targetTPS;
        }
        
        public int getTargetTPS(){
            return this.targetTPS;
        }
	
	public String getDBPassword() {
		return this.db_password;
	}

	public void setDBDriver(String driver) {
		this.db_driver = driver;
	}
	
	public String getDBDriver() {
		return this.db_driver;
	}
        
        public boolean getCalibrate(){
            return this.calibrate;
        }
        
        public boolean getGeneratesFiles(){
            return this.generateFiles;
        }
        
        public String getFilesPath(){
            return this.filesPath;
        }
        
        public void setGenerateFiles(boolean gen){
            this.generateFiles=gen;
        }
        
        public void setFilesPath(String filesPath){
            this.filesPath=filesPath;
        }
        
        public void setCalibrate(boolean calibrate){
            this.calibrate=calibrate;
        }
	
	public void setRecordAbortMessages(boolean recordAbortMessages) {
        this.recordAbortMessages = recordAbortMessages;
    }
	
	/**
	 * Whether each worker should record the transaction's UserAbort messages
	 * This primarily useful for debugging a benchmark
	 */
	public boolean getRecordAbortMessages() {
        return (this.recordAbortMessages);
    }
	
	/**
	 * Set the scale factor for the database
	 * A value of 1 means the default size.
	 * A value greater than 1 means the database is larger
	 * A value less than 1 means the database is smaller 
	 * @param scaleFactor
	 */
	public void setScaleFactor(double scaleFactor) {
        this.scaleFactor = scaleFactor;
    }
	/**
	 * Return the scale factor of the database size
	 * @return
	 */
	public double getScaleFactor() {
        return this.scaleFactor;
    }

	/**
	 * Return the number of phases specified in the config file
	 * @return
	 */
	public int getNumberOfPhases() {
		return this.numberOfPhases;
	}

	/**
     * Set the directory in which we can find the data files (for example, CSV
     * files) for loading the database.
     */ 
    public void setDataDir(String dir) {
        this.dataDir = dir;
    }

    /**
     * Return the directory in which we can find the data files (for example, CSV
     * files) for loading the database.
     */ 
    public String getDataDir() {
        return this.dataDir;
    }

    /**
	 * A utility method that init the phaseIterator and dialectMap
	 */
	public void init() {
	    try {
	        Class.forName(this.db_driver);
	    } catch (ClassNotFoundException ex) {
	        throw new RuntimeException("Failed to initialize JDBC driver '" + this.db_driver + "'", ex);
	    }
	}

	public void setTerminals(int terminals) {
		this.terminals = terminals;
	}
        
        
	public void setOLAPTerminals(int terminals) {
		this.OLAPterminals = terminals;
	}

	public int getTerminals() {
		return terminals;
	}
        
        public int getOLAPTerminals() {
		return OLAPterminals;
	}
	
	public TransactionTypes getTransTypes() {
		return transTypes;
	}

	public void setTransTypes(TransactionTypes transTypes) {
		this.transTypes = transTypes;
	}

	public List<Phase> getAllPhases() {
		return works;
	}

	public void setXmlConfig(XMLConfiguration xmlConfig) {
		this.xmlConfig = xmlConfig;
	}

	public XMLConfiguration getXmlConfig() {
		return xmlConfig;
	}

	public int getIsolationMode() {
		return isolationMode;
	}

    public String getIsolationString() {
        if(this.isolationMode== Connection.TRANSACTION_SERIALIZABLE)
            return "TRANSACTION_SERIALIZABLE";
        else if(this.isolationMode==Connection.TRANSACTION_READ_COMMITTED)
            return "TRANSACTION_READ_COMMITTED";
        else if(this.isolationMode==Connection.TRANSACTION_REPEATABLE_READ)
            return "TRANSACTION_REPEATABLE_READ";
        else if(this.isolationMode==Connection.TRANSACTION_READ_UNCOMMITTED)
            return "TRANSACTION_READ_UNCOMMITTED";
        else
            return "TRANSACTION_SERIALIZABLE [DEFAULT]";
    }

	public void setIsolationMode(String mode) {
		if(mode.equals("TRANSACTION_SERIALIZABLE"))
			this.isolationMode= Connection.TRANSACTION_SERIALIZABLE;
		else if(mode.equals("TRANSACTION_READ_COMMITTED"))
			this.isolationMode=Connection.TRANSACTION_READ_COMMITTED;
		else if(mode.equals("TRANSACTION_REPEATABLE_READ"))
			this.isolationMode=Connection.TRANSACTION_REPEATABLE_READ;
		else if(mode.equals("TRANSACTION_READ_UNCOMMITTED"))
			this.isolationMode=Connection.TRANSACTION_READ_UNCOMMITTED;
		else if(!mode.isEmpty())
			System.out.println("Indefined isolation mode, set to default [TRANSACTION_SERIALIZABLE]");
	}
	
	@Override
	public String toString() {
        Class<?> confClass = this.getClass();
        Map<String, Object> m = new ListOrderedMap<String, Object>();
        for (Field f : confClass.getDeclaredFields()) {
            Object obj = null;
            try {
                obj = f.get(this);
            } catch (IllegalAccessException ex) {
                throw new RuntimeException(ex);
            }
            m.put(f.getName().toUpperCase(), obj);
        } // FOR
        return StringUtil.formatMaps(m);
    }
}
