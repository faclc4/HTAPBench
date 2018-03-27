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
package pt.haslab.htapbench.api;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.log4j.Logger;
import pt.haslab.htapbench.catalog.Catalog;

import pt.haslab.htapbench.core.WorkloadConfiguration;
import pt.haslab.htapbench.densitity.Clock;
import pt.haslab.htapbench.types.DatabaseType;
import pt.haslab.htapbench.util.ClassUtil;
import pt.haslab.htapbench.util.ScriptRunner;

/**
 * Base class for all benchmark implementations
 */
public abstract class BenchmarkModule {
    private static final Logger LOG = Logger.getLogger(BenchmarkModule.class);

    protected final String benchmarkName;

    /**
     * The workload configuration for this benchmark invocation
     */
    protected WorkloadConfiguration workConf;

    /**
     * These are the variations of the Procedure's Statment SQL
     */
    protected final StatementDialects dialects;

    /**
     * Database Catalog
     */
    protected final Catalog catalog;

    /**
     * Supplemental Procedures
     */
    private final Set<Class<? extends Procedure>> supplementalProcedures = new HashSet<Class<? extends Procedure>>();

    /**
     * The last Connection that was created using this BenchmarkModule
     */
    private Connection last_connection;

    /**
     * A single Random object that should be re-used by all a benchmark's components
     */
    private final Random rng = new Random();

    /**
     * Whether to use verbose output messages
     * @deprecated
     */
    protected boolean verbose;

    public BenchmarkModule(String benchmarkName, WorkloadConfiguration workConf, boolean withCatalog) {
        assert (workConf != null) : "The WorkloadConfiguration instance is null.";

        this.benchmarkName = benchmarkName;
        this.workConf = workConf;
        this.catalog = (withCatalog ? new Catalog(this) : null);
        File xmlFile = this.getSQLDialect();
        this.dialects = new StatementDialects(this.workConf.getDBType(), xmlFile);
    }

    // --------------------------------------------------------------------------
    // DATABASE CONNETION
    // --------------------------------------------------------------------------

    /**
     * 
     * @return
     * @throws SQLException
     */
    public final Connection makeConnection() throws SQLException {
        Connection conn = DriverManager.getConnection(workConf.getDBConnection(),
        workConf.getDBUsername(),
        workConf.getDBPassword());
        Catalog.setSeparator(conn);
        this.last_connection = conn;
        return (conn);
    }

    /**
     * Return the last Connection handle created by this BenchmarkModule
     * @return
     */
    protected final Connection getLastConnection() {
        return (this.last_connection);
    }

    // --------------------------------------------------------------------------
    // IMPLEMENTING CLASS INTERFACE
    // --------------------------------------------------------------------------
    protected abstract List<Worker> makeWorkersImpl(boolean verbose,String workerType,Clock clock) throws IOException;

    protected abstract List<Worker> makeOLAPWorkerImpl(boolean verbose, Clock clock) throws IOException;
    /**
     * Each BenchmarkModule needs to implement this method to load a sample
     * dataset into the database. The Connection handle will already be
     * configured for you, and the base class will commit+close it once this
     * method returns
     * 
     * @param conn
     *            TODO
     * @return TODO
     * @throws SQLException
     *             TODO
     */
    protected abstract Loader makeLoaderImpl(Connection conn, boolean calibrate, boolean generateFiles, String filePath) throws SQLException;

    protected abstract Package getProcedurePackageImpl();
    
    protected abstract Package getProcedurePackageImpl(String txnName);

    // --------------------------------------------------------------------------
    // PUBLIC INTERFACE
    // --------------------------------------------------------------------------

    /**
     * Return the Random generator that should be used by all this benchmark's components
     */
    public Random rng() {
        return (this.rng);
    }

    /**
     * 
     * @return
     */
    public URL getDatabaseDDL() {
        return (this.getDatabaseDDL(this.workConf.getDBType()));
    }

    /**
     * Return the URL handle to the DDL used to load the benchmark's database
     * schema.
     * @param conn 
     * @throws SQLException 
     */
    public URL getDatabaseDDL(DatabaseType db_type) {
        String ddlNames[] = {
                this.benchmarkName + "-" + (db_type != null ? db_type.name().toLowerCase() : "") + "-ddl.sql",
                this.benchmarkName + "-ddl.sql",
        };

        for (String ddlName : ddlNames) {
            if (ddlName == null) continue;
            URL ddlURL = this.getClass().getResource(ddlName);
            if (ddlURL != null) return ddlURL;
        } // FOR
        LOG.trace(ddlNames[0]+" :or: "+ddlNames[1]);
        LOG.error("Failed to find DDL file for " + this.benchmarkName);
        return null;
    }

    /**
     * Return the File handle to the SQL Dialect XML file
     * used for this benchmark 
     * @return
     */
    public File getSQLDialect() {
        String xmlName = this.benchmarkName + "-dialects.xml";
        URL ddlURL = this.getClass().getResource(xmlName);
        if (ddlURL != null) return new File(ddlURL.getPath());
        if (LOG.isDebugEnabled())
            LOG.warn(String.format("Failed to find SQL Dialect XML file '%s'", xmlName));
        return (null);
    }

    /**
     * Calls the implementation for making all the OLTP Workers.
     * @param verbose
     * @param workerType
     * @return
     * @throws IOException 
     */
    public final List<Worker> makeWorkers(boolean verbose,String workerType,Clock clock) throws IOException {
        return (this.makeWorkersImpl(verbose,workerType,clock));
    }
    
    /**
     * Calls the implementation for making a OLAP Worker.
     * @param verbose
     * @return
     * @throws IOException 
     */
    public final List<Worker> makeOLAPWorker(boolean verbose,Clock clock) throws IOException {
        return (this.makeOLAPWorkerImpl(verbose,clock));
    }

    /**
     * Create the Benchmark Database
     * This is the main method used to create all the database 
     * objects (e.g., table, indexes, etc) needed for this benchmark 
     */
    public final void createDatabase() {
        try {
            Connection conn = this.makeConnection();
            this.createDatabase(this.workConf.getDBType(), conn);
            conn.close();
        } catch (SQLException ex) {
            throw new RuntimeException(String.format("Unexpected error when trying to create the %s database", this.benchmarkName), ex);
        }
    }

    /**
     * Create the Benchmark Database
     * This is the main method used to create all the database 
     * objects (e.g., table, indexes, etc) needed for this benchmark 
     */
    public final void createDatabase(DatabaseType dbType, Connection conn) throws SQLException {
        try {
            URL ddl = this.getDatabaseDDL(dbType);
            assert(ddl != null) : "Failed to get DDL for " + this;
            ScriptRunner runner = new ScriptRunner(conn, true, true);
            if (LOG.isDebugEnabled()) LOG.debug("Executing script '" + ddl + "'");
            runner.runScript(ddl);
        } catch (Exception ex) {
            throw new RuntimeException(String.format("Unexpected error when trying to create the %s database", this.benchmarkName), ex);
        }
    }

    /**
     * Run a script on a Database
     */
    public final void runScript(String script) {
        try {
            Connection conn = this.makeConnection();
            ScriptRunner runner = new ScriptRunner(conn, true, true);
            File scriptFile= new File(script);
            runner.runScript(scriptFile.toURI().toURL());
            conn.close();
        } catch (SQLException ex) {
            throw new RuntimeException(String.format("Unexpected error when trying to run: %s", script), ex);
        } catch (IOException ex) {
            throw new RuntimeException(String.format("Unexpected error when trying to open: %s", script), ex);
        }
    }

    /**
     * Invoke this benchmark's database loader
     */
    public final void loadDatabase(boolean calibrate,boolean generateFiles, String filesPath) {
        try {
            Connection conn = null;
            if(!generateFiles){
                conn = this.makeConnection();
            }
            this.loadDatabase(conn,calibrate,generateFiles,filesPath);
            if(conn !=null){
                conn.close();
            }
        } catch (SQLException ex) {
            throw new RuntimeException(String.format("Unexpected error when trying to load the %s database", this.benchmarkName), ex);
        }
    }
    
    /**
     * Invoke this benchmark's database loader
     */
    public final void generateCSVDatabase(boolean calibrate,boolean generateFiles, String filesPath) {
            this.loadDatabase(null,calibrate,generateFiles,filesPath);

    }

    /**
     * Invoke this benchmark's database loader using the given Connection handle
     * @param conn
     */
    protected final void loadDatabase(Connection conn, boolean calibrate, boolean generateFiles, String filesPath) {
        try {
            Loader loader = this.makeLoaderImpl(conn, calibrate,generateFiles,filesPath);
            if (loader != null) {
                if(conn!=null){
                    conn.setAutoCommit(false);
                }
                loader.load();
                if(conn!= null){
                    conn.commit();
                }

                if (loader.getTableCounts().isEmpty() == false) {
                    LOG.info("Table Counts:\n" + loader.getTableCounts());
                }
            }
        } catch (SQLException ex) {
            throw new RuntimeException(String.format("Unexpected error when trying to load the %s database", this.benchmarkName), ex);
        }
    }

    /**
     * @param DB_CONN
     * @throws SQLException
     */
    public final void clearDatabase() {
        try {
            Connection conn = this.makeConnection();
            Loader loader = this.makeLoaderImpl(conn, false,false,"");
            if (loader != null) {
                conn.setAutoCommit(false);
                loader.unload(this.catalog);
                conn.commit();
            }
        } catch (SQLException ex) {
            throw new RuntimeException(String.format("Unexpected error when trying to delete the %s database", this.benchmarkName), ex);
        }
    }

    // --------------------------------------------------------------------------
    // UTILITY METHODS
    // --------------------------------------------------------------------------

    /**
     * Return the unique identifier for this benchmark
     */
    public final String getBenchmarkName() {
        return (this.benchmarkName);
    }
    /**
     * Return the database's catalog
     */
    public final Catalog getCatalog() {
        return (this.catalog);
    }
    /**
     * Return the StatementDialects loaded for this benchmark
     */
    public final StatementDialects getStatementDialects() {
        return (this.dialects);
    }
    @Override
    public final String toString() {
        return benchmarkName.toUpperCase();
    }

    /**
     * Initialize a TransactionType handle for the get procedure name and id
     * This should only be invoked a start-up time
     * @param procName
     * @param id
     * @return
     */
    @SuppressWarnings("unchecked")
    public final TransactionType initTransactionType(String procName, int id) {
        if (id == TransactionType.INVALID_ID) {
            LOG.error(String.format("Procedure %s.%s cannot the reserved id '%d' for %s",
                    this.benchmarkName, procName, id,
                    TransactionType.INVALID.getClass().getSimpleName()));
            return null;
        }

        //Package pkg = this.getProcedurePackageImpl();
        Package pkg = this.getProcedurePackageImpl(procName);
        assert (pkg != null) : "Null Procedure package for " + this.benchmarkName;
        String fullName = pkg.getName() + "." + procName;
        Class<? extends Procedure> procClass = (Class<? extends Procedure>) ClassUtil.getClass(fullName);
        assert (procClass != null) : "Unexpected Procedure name " + this.benchmarkName + "." + procName;
        return new TransactionType(procClass, id);
    }

    public final WorkloadConfiguration getWorkloadConfiguration() {
        return (this.workConf);
    }

    /**
     * Return a mapping from TransactionTypes to Procedure invocations
     * @param txns
     * @param pkg
     * @return
     */
    public Map<TransactionType, Procedure> getProcedures() {
        Map<TransactionType, Procedure> proc_xref = new HashMap<TransactionType, Procedure>();
        TransactionTypes txns = this.workConf.getTransTypes();

        if (txns != null) {
            for (Class<? extends Procedure> procClass : this.supplementalProcedures) {
                TransactionType txn = txns.getType(procClass);
                if (txn == null) {
                    txn = new TransactionType(procClass, procClass.hashCode(), true);
                    txns.add(txn);
                }
            } // FOR

            for (TransactionType txn : txns) {
                Procedure proc = (Procedure)ClassUtil.newInstance(txn.getProcedureClass(),
                        new Object[0],
                        new Class<?>[0]);
                proc.initialize(this.workConf.getDBType());
                proc_xref.put(txn, proc);
                proc.loadSQLDialect(this.dialects);
            } // FOR
        }
        if (proc_xref.isEmpty()) {
            LOG.warn("No procedures defined for " + this);
        }
        return (proc_xref);
    }

    /**
     * 
     * @param procClass
     */
    public final void registerSupplementalProcedure(Class<? extends Procedure> procClass) {
        this.supplementalProcedures.add(procClass);
    }

    public void setWorkloadConfiguration(WorkloadConfiguration workConf){
        this.workConf=workConf;
    }
}
