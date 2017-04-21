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

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.SubnodeConfiguration;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.configuration.tree.xpath.XPathExpressionEngine;
import org.apache.log4j.Logger;

import pt.haslab.htapbench.api.BenchmarkModule;
import pt.haslab.htapbench.api.TransactionType;
import pt.haslab.htapbench.api.TransactionTypes;
import pt.haslab.htapbench.api.Worker;
import pt.haslab.htapbench.benchmark.HTAPBenchmark;
import pt.haslab.htapbench.densitity.Clock;
import pt.haslab.htapbench.types.DatabaseType;
import pt.haslab.htapbench.util.FileUtil;
import pt.haslab.htapbench.util.QueueLimitException;
import pt.haslab.htapbench.util.ResultUploader;
import pt.haslab.htapbench.util.StringUtil;
import pt.haslab.htapbench.util.TimeUtil;

public class HTAPBench {
    private static final Logger LOG = Logger.getLogger(HTAPBench.class);
    
    private static final String SINGLE_LINE = "**********************************************************************************";
    
    private static final String RATE_DISABLED = "disabled";
    private static final String RATE_UNLIMITED = "unlimited";
    
    /**
     * @param args
     * @throws Exception 
     */
    public static void main(String[] args) throws Exception {        
        // Initialize log4j
        String log4jPath = "./log4j.configuration";
        if (log4jPath != null) {
            org.apache.log4j.PropertyConfigurator.configure(log4jPath);
        } else {
            throw new RuntimeException("Missing log4j.properties file");
        }
        
        // create the command line parser
        CommandLineParser parser = new PosixParser();
        XMLConfiguration pluginConfig=null;
        try {
            pluginConfig = new XMLConfiguration("./config/plugin.xml");
        } catch (ConfigurationException e1) {
            LOG.info("Plugin configuration file config/plugin.xml is missing");
            e1.printStackTrace();
        }
        pluginConfig.setExpressionEngine(new XPathExpressionEngine());
        Options options = new Options();
        options.addOption(
                "b",
                "bench",
                true,
                "[required] Benchmark class. Currently supported: "+ pluginConfig.getList("/plugin/@name"));
        options.addOption(
                "c", 
                "config", 
                true,
                "[required] Workload configuration file");
        options.addOption(
                null,
                "create",
                true,
                "Initialize the database for this benchmark");
        options.addOption(
                null,
                "clear",
                true,
                "Clear all records in the database for this benchmark");
        options.addOption(
                null,
                "load",
                true,
                "Load data using the benchmark's data loader");
        options.addOption(
                null,
                "generateFiles",
                true,
                "Generate CSV Files to Populate Database");
        options.addOption(
                null,
                "filePath",
                true,
                "Path to generate the CSV files to load the Database.");
        options.addOption(
                null,
                "execute",
                true,
                "Execute the benchmark workload");
        options.addOption(
                null,
                "runscript",
                true,
                "Run an SQL script");
        options.addOption(
                null,
                "upload",
                true,
                "Upload the result");
        options.addOption(
                null,
                "calibrate",
                true,
                "Extracts the parameter densities from the load process.");
        options.addOption(
                null,
                "oltp",
                true,
                "Runs only the OLTP stage of the benchmark.");
        options.addOption(
                null,
                "olap",
                true,
                "Runs only the OLAP stage of the benchmark.");
        

        options.addOption("v", "verbose", false, "Display Messages");
        options.addOption("h", "help", false, "Print this help");
        options.addOption("s", "sample", true, "Sampling window");
        options.addOption("im", "interval-monitor", true, "Throughput Monitoring Interval in seconds");
        options.addOption("ss", false, "Verbose Sampling per Transaction");
        options.addOption("o", "output", true, "Output file (default System.out)");
        options.addOption("d", "directory", true, "Base directory for the result files, default is current directory");
        options.addOption("t", "timestamp", false, "Each result file is prepended with a timestamp for the beginning of the experiment");
        options.addOption("ts", "tracescript", true, "Script of transactions to execute");
        options.addOption(null, "histograms", false, "Print txn histograms");
        options.addOption(null, "dialects-export", true, "Export benchmark SQL to a dialects file");

        // parse the command line arguments
        CommandLine argsLine = parser.parse(options, args);
        if (argsLine.hasOption("h")) {
            printUsage(options);
            return;
        } else if (argsLine.hasOption("c") == false) {
            LOG.error("Missing Configuration file");
            printUsage(options);
            return;
        } else if (argsLine.hasOption("b") == false) {
            LOG.fatal("Missing Benchmark Class to load");
            printUsage(options);
            return;
        }
        
        // If an output directory is used, store the information
        String outputDirectory = "";
        if (argsLine.hasOption("d")) {
            outputDirectory = argsLine.getOptionValue("d");
        }
        
        String timestampValue = "";
        if (argsLine.hasOption("t")) {
            timestampValue = String.valueOf(TimeUtil.getCurrentTime().getTime()) + "_";
        }
        
        // Seconds
        int intervalMonitor = 0;
        if (argsLine.hasOption("im")) {
            intervalMonitor = Integer.parseInt(argsLine.getOptionValue("im"));
        }
        
        // -------------------------------------------------------------------
        // GET PLUGIN LIST
        // -------------------------------------------------------------------
        
        String targetBenchmarks = argsLine.getOptionValue("b");
        
        String[] targetList = targetBenchmarks.split(",");
        List<BenchmarkModule> benchList = new ArrayList<BenchmarkModule>();
        
        // Use this list for filtering of the output
        List<TransactionType> activeTXTypes = new ArrayList<TransactionType>();
        
        String configFile = argsLine.getOptionValue("c");
        XMLConfiguration xmlConfig = new XMLConfiguration(configFile);
        xmlConfig.setExpressionEngine(new XPathExpressionEngine());
        
        String generateFilesPath = argsLine.getOptionValue("filePath");
        
        WorkloadSetup setup = new WorkloadSetup(xmlConfig);
        //HTAPB:
        //Workload setup : automatic computation of the #wh and terminals based on the ideal client:

        setup.computeWorkloadSetup();        

        // Load the configuration for each benchmark
        int lastTxnId = 0;
        double error_margin = 0;
        
        for (String plugin : targetList) {
            String pluginTest = "[@bench='" + plugin + "']";

            // ----------------------------------------------------------------
            // BEGIN LOADING WORKLOAD CONFIGURATION
            // ----------------------------------------------------------------
            
            WorkloadConfiguration wrkld = new WorkloadConfiguration();
            wrkld.setBenchmarkName(plugin);
            wrkld.setXmlConfig(xmlConfig);
            boolean scriptRun = false;
            if (argsLine.hasOption("t")) {
                scriptRun = true;
                String traceFile = argsLine.getOptionValue("t");
                wrkld.setTraceReader(new TraceReader(traceFile));
                if (LOG.isDebugEnabled()) LOG.debug(wrkld.getTraceReader().toString());
            }

            // Pull in database configuration
            wrkld.setDBType(DatabaseType.get(xmlConfig.getString("dbtype")));
            wrkld.setDBDriver(xmlConfig.getString("driver"));
            wrkld.setDBConnection(xmlConfig.getString("DBUrl"));
            wrkld.setDBName(xmlConfig.getString("DBName"));
            wrkld.setDBUsername(xmlConfig.getString("username"));
            wrkld.setDBPassword(xmlConfig.getString("password"));
            String isolationMode = xmlConfig.getString("isolation[not(@bench)]", "TRANSACTION_SERIALIZABLE");
            wrkld.setIsolationMode(xmlConfig.getString("isolation" + pluginTest, isolationMode));
            wrkld.setRecordAbortMessages(xmlConfig.getBoolean("recordabortmessages", false));
            wrkld.setDataDir(xmlConfig.getString("datadir", "."));
            if(options.getOption("calibrate").hasArg()){
                wrkld.setCalibrate(true);
            }
            if(options.getOption("generateFiles").hasArg()){
                wrkld.setGenerateFiles(Boolean.parseBoolean(argsLine.getOptionValue("generateFiles")));   
            }
            if(options.getOption("filePath").hasArg()){
                    wrkld.setFilesPath(generateFilesPath);
            }
            
            int terminals;
            boolean rateLimited;
            
            
            //OLTP
            if(isBooleanOptionSet(argsLine, "oltp")){
                wrkld.setScaleFactor(xmlConfig.getDouble("warehouses"));
                terminals = (int)xmlConfig.getDouble("warehouses")*10;
                wrkld.setTerminals(terminals);    
                rateLimited = false;
                //NoTarget
            }
            //OLAP
            else if(isBooleanOptionSet(argsLine, "olap")){
                wrkld.setScaleFactor(xmlConfig.getDouble("warehouses"));
                terminals = (int)xmlConfig.getDouble("OLAP_workers");
                wrkld.setOLAPTerminals(terminals);
                rateLimited = false;
            }
            else{
                //HTAP
                wrkld.setTerminals(setup.getTerminals());
                terminals=setup.getTerminals();
                wrkld.setOLAPTerminals(1);
                wrkld.setScaleFactor(setup.getWarehouses());
                wrkld.setTargetTPS(setup.getTargetTPS());
                error_margin = (double)xmlConfig.getDouble("error_margin");
                rateLimited = true;
            }
            
            
            LOG.debug("");
            LOG.debug("------------- Workload properties --------------------");
            LOG.debug("      Target TPS: "+ setup.getTargetTPS());
            LOG.debug("      # warehouses: "+ setup.getWarehouses());
            LOG.debug("      #terminals: "+ setup.getTerminals());
            LOG.debug("------------------------------------------------------");
            
            // ----------------------------------------------------------------
            // CREATE BENCHMARK MODULE
            // ----------------------------------------------------------------

            String classname = pluginConfig.getString("/plugin[@name='" + plugin + "']");

            if (classname == null)
                throw new ParseException("Plugin " + plugin + " is undefined in config/plugin.xml");
            BenchmarkModule bench = new HTAPBenchmark(wrkld);
            Map<String, Object> initDebug = new ListOrderedMap<String, Object>();
            initDebug.put("Benchmark", String.format("%s {%s}", plugin.toUpperCase(), classname));
            initDebug.put("Configuration", configFile);
            initDebug.put("Type", wrkld.getDBType());
            initDebug.put("Driver", wrkld.getDBDriver());
            initDebug.put("URL", wrkld.getDBConnection());
            initDebug.put("Isolation", wrkld.getIsolationString());
            initDebug.put("Scale Factor", wrkld.getScaleFactor());
            LOG.info(SINGLE_LINE + "\n\n" + StringUtil.formatMaps(initDebug));
            LOG.info(SINGLE_LINE);

            // ----------------------------------------------------------------
            // LOAD TRANSACTION DESCRIPTIONS
            // ----------------------------------------------------------------
            int numTxnTypes = xmlConfig.configurationsAt("transactiontypes" + pluginTest + "/transactiontype").size();
            if (numTxnTypes == 0 && targetList.length == 1) {
                //if it is a single workload run, <transactiontypes /> w/o attribute is used
                pluginTest = "[not(@bench)]";
                numTxnTypes = xmlConfig.configurationsAt("transactiontypes" + pluginTest + "/transactiontype").size();
            }
            wrkld.setNumTxnTypes(numTxnTypes);

            List<TransactionType> ttypes = new ArrayList<TransactionType>();
            ttypes.add(TransactionType.INVALID);
            int txnIdOffset = lastTxnId;
            for (int i = 1; i < wrkld.getNumTxnTypes() + 1; i++) {
                String key = "transactiontypes" + pluginTest + "/transactiontype[" + i + "]";
                String txnName = xmlConfig.getString(key + "/name");

                // Get ID if specified; else increment from last one.
                int txnId = i + 1;
                if (xmlConfig.containsKey(key + "/id")) {
                    txnId = xmlConfig.getInt(key + "/id");
                }

                TransactionType tmpType = bench.initTransactionType(txnName, txnId + txnIdOffset);

                // Keep a reference for filtering
                activeTXTypes.add(tmpType);

                // Add a ref for the active TTypes in this benchmark
                ttypes.add(tmpType);
                lastTxnId = i;
            } // FOR

            // Wrap the list of transactions and save them
            TransactionTypes tt = new TransactionTypes(ttypes);
            wrkld.setTransTypes(tt);
            LOG.debug("Using the following transaction types: " + tt);

            // Read in the groupings of transactions (if any) defined for this
            // benchmark
            HashMap<String,List<String>> groupings = new HashMap<String,List<String>>();
            int numGroupings = xmlConfig.configurationsAt("transactiontypes" + pluginTest + "/groupings/grouping").size();
            LOG.debug("Num groupings: " + numGroupings);
            for (int i = 1; i < numGroupings + 1; i++) {
                String key = "transactiontypes" + pluginTest + "/groupings/grouping[" + i + "]";

                // Get the name for the grouping and make sure it's valid.
                String groupingName = xmlConfig.getString(key + "/name").toLowerCase();
                if (!groupingName.matches("^[a-z]\\w*$")) {
                    LOG.fatal(String.format("Grouping name \"%s\" is invalid."
                                + " Must begin with a letter and contain only"
                                + " alphanumeric characters.", groupingName));
                    System.exit(-1);
                }
                else if (groupingName.equals("all")) {
                    LOG.fatal("Grouping name \"all\" is reserved."
                              + " Please pick a different name.");
                    System.exit(-1);
                }

                // Get the weights for this grouping and make sure that there
                // is an appropriate number of them.
                List<String> groupingWeights = xmlConfig.getList(key + "/weights");
                if (groupingWeights.size() != numTxnTypes) {
                    LOG.fatal(String.format("Grouping \"%s\" has %d weights,"
                                + " but there are %d transactions in this"
                                + " benchmark.", groupingName,
                                groupingWeights.size(), numTxnTypes));
                    System.exit(-1);
                }

                LOG.debug("Creating grouping with name, weights: " + groupingName + ", " + groupingWeights);
                groupings.put(groupingName, groupingWeights);
            }

            // All benchmarks should also have an "all" grouping that gives
            // even weight to all transactions in the benchmark.
            List<String> weightAll = new ArrayList<String>();
            for (int i = 0; i < numTxnTypes; ++i)
                weightAll.add("1");
            groupings.put("all", weightAll);
            
            WorkloadConfiguration workConf = bench.getWorkloadConfiguration();
            workConf.setScaleFactor(setup.getWarehouses());
            workConf.setTerminals(setup.getTerminals());
            workConf.setTargetTPS(setup.getTargetTPS());
            workConf.setFilesPath(generateFilesPath);
            bench.setWorkloadConfiguration(workConf);
            
            benchList.add(bench);

            // ----------------------------------------------------------------
            // WORKLOAD CONFIGURATION
            // ----------------------------------------------------------------
            
            int size = xmlConfig.configurationsAt("/works/work").size();
            for (int i = 1; i < size + 1; i++) {
                SubnodeConfiguration work = xmlConfig.configurationAt("works/work[" + i + "]");
                List<String> weight_strings;
                
                // use a workaround if there multiple workloads or single
                // attributed workload
                if (targetList.length > 1 || work.containsKey("weights[@bench]")) {
                    String weightKey = work.getString("weights" + pluginTest).toLowerCase();
                    if (groupings.containsKey(weightKey))
                        weight_strings = groupings.get(weightKey);
                    else
                    weight_strings = get_weights(plugin, work);
                } else {
                    String weightKey = work.getString("weights[not(@bench)]").toLowerCase();
                    if (groupings.containsKey(weightKey))
                        weight_strings = groupings.get(weightKey);
                    else
                    weight_strings = work.getList("weights[not(@bench)]"); 
                }
                
                boolean disabled = false;
                boolean serial = false;
                boolean timed = false;

                Phase.Arrival arrival=Phase.Arrival.REGULAR;
                String arrive=work.getString("@arrival","regular");
                if(arrive.toUpperCase().equals("POISSON"))
                    arrival=Phase.Arrival.POISSON;
                
                // We now have the option to run all queries exactly once in
                // a serial (rather than random) order.
                String serial_string;
                serial_string = work.getString("serial", "false");
                if (serial_string.equals("true")) {
                    serial = true;
                }
                else if (serial_string.equals("false")) {
                    serial = false;
                }
                else {
                    LOG.fatal("Serial string should be either 'true' or 'false'.");
                    System.exit(-1);
                }

                // We're not actually serial if we're running a script, so make
                // sure to suppress the serial flag in this case.
                serial = serial && (wrkld.getTraceReader() == null);

                int activeTerminals;
                activeTerminals = work.getInt("active_terminals[not(@bench)]", terminals);
                activeTerminals = work.getInt("active_terminals" + pluginTest, activeTerminals);
                // If using serial, we should have only one terminal
                if (serial && activeTerminals != 1) {
                    LOG.warn("Serial ordering is enabled, so # of active terminals is clamped to 1.");
                    activeTerminals = 1;
                }
                if (activeTerminals > terminals) {
                    LOG.error(String.format("Configuration error in work %d: " +
                                            "Number of active terminals is bigger than the total number of terminals",
                              i));
                    System.exit(-1);
                }

                int time = work.getInt("/time", 0);
                timed = (time > 0);
                if (scriptRun) {
                    LOG.info("Running a script; ignoring timer, serial, and weight settings.");
                }
                else if (!timed) {
                    if (serial)
                        LOG.info("Timer disabled for serial run; will execute"
                                 + " all queries exactly once.");
                    else {
                        LOG.fatal("Must provide positive time bound for"
                                  + " non-serial executions. Either provide"
                                  + " a valid time or enable serial mode.");
                        System.exit(-1);
                    }
                }
                else if (serial)
                    LOG.info("Timer enabled for serial run; will run queries"
                             + " serially in a loop until the timer expires.");

                wrkld.addWork(time,
                              setup.getTargetTPS(),
                              weight_strings,
                              rateLimited,
                              disabled,
                              serial,
                              timed,
                              activeTerminals,
                              arrival);
            } // FOR
    
            // CHECKING INPUT PHASES
            int j = 0;
            for (Phase p : wrkld.getAllPhases()) {
                j++;
                if (p.getWeightCount() != wrkld.getNumTxnTypes()) {
                    LOG.fatal(String.format("Configuration files is inconsistent, phase %d contains %d weights but you defined %d transaction types",
                                            j, p.getWeightCount(), wrkld.getNumTxnTypes()));
                    if (p.isSerial()) {
                        LOG.fatal("However, note that since this a serial phase, the weights are irrelevant (but still must be included---sorry).");
                    }
                    System.exit(-1);
                }
            } // FOR
    
            // Generate the dialect map
            wrkld.init();
    
            assert (wrkld.getNumTxnTypes() >= 0);
            assert (xmlConfig != null);
        }
        assert(benchList.isEmpty() == false);
        assert(benchList.get(0) != null);
        
        BenchmarkModule bench = benchList.get(0);
        // Export StatementDialects
        if (isBooleanOptionSet(argsLine, "dialects-export")) {
            if (bench.getStatementDialects() != null) {
                LOG.info("Exporting StatementDialects for " + bench);
                String xml = bench.getStatementDialects().export(bench.getWorkloadConfiguration().getDBType(),
                                                                 bench.getProcedures().values());
                System.out.println(xml);
                System.exit(0);
            }
            throw new RuntimeException("No StatementDialects is available for " + bench);
        }

        
        @Deprecated
        boolean verbose = argsLine.hasOption("v");

        // Create the Benchmark's Database
        if (isBooleanOptionSet(argsLine, "create")) {
            for (BenchmarkModule benchmark : benchList) {
                LOG.info("Creating new " + benchmark.getBenchmarkName().toUpperCase() + " database...");
                runCreator(benchmark, verbose);
                LOG.info("Finished!");
                LOG.info(SINGLE_LINE);
            }
        } else if (LOG.isDebugEnabled()) {
            LOG.debug("Skipping creating benchmark database tables");
            LOG.info(SINGLE_LINE);
        }

        // Clear the Benchmark's Database
        if (isBooleanOptionSet(argsLine, "clear")) {
                for (BenchmarkModule benchmark : benchList) {
                LOG.info("Resetting " + benchmark.getBenchmarkName().toUpperCase() + " database...");
                benchmark.clearDatabase();
                LOG.info("Finished!");
                LOG.info(SINGLE_LINE);
            }
        } else if (LOG.isDebugEnabled()) {
            LOG.debug("Skipping creating benchmark database tables");
            LOG.info(SINGLE_LINE);
        }

        //Generate Files
        if (isBooleanOptionSet(argsLine, "generateFiles")) {    
                for (BenchmarkModule benchmark : benchList) {
                        LOG.info("Generate Files: "+benchmark.getWorkloadConfiguration().getGeneratesFiles() );
                        LOG.info("File Path: "+ benchmark.getWorkloadConfiguration().getFilesPath());
                        runLoader(benchmark, verbose, true,true,bench.getWorkloadConfiguration().getFilesPath());
                        LOG.info("Finished!");
                        LOG.info(SINGLE_LINE);
                    }
            }
        
        // Execute Loader
        if (isBooleanOptionSet(argsLine, "load")) {
            if (isBooleanOptionSet(argsLine, "calibrate")) {    
                for (BenchmarkModule benchmark : benchList) {
                        LOG.info("Loading data into " + benchmark.getBenchmarkName().toUpperCase() + " database...");
                        runLoader(benchmark, verbose, true,false,"null");
                        LOG.info("Finished!");
                        LOG.info(SINGLE_LINE);
                    }
            }
            else{
                for (BenchmarkModule benchmark : benchList) {
                    LOG.info("Loading data into " + benchmark.getBenchmarkName().toUpperCase() + " database...");
                    runLoader(benchmark, verbose,false,false,"null");
                    LOG.info("Finished!");
                    LOG.info(SINGLE_LINE);
                }
            }
        } else if (LOG.isDebugEnabled()) {
            LOG.debug("Skipping loading benchmark database records");
            LOG.info(SINGLE_LINE);
        }
        
        // Execute a Script
        if (argsLine.hasOption("runscript")) {
            for (BenchmarkModule benchmark : benchList) {
                String script = argsLine.getOptionValue("runscript");
                LOG.info("Running a SQL script: "+script);
                runScript(benchmark, script);
                LOG.info("Finished!");
                LOG.info(SINGLE_LINE);
            }
        }

        // Execute Workload
        if (isBooleanOptionSet(argsLine, "execute")) {
            // Bombs away!
            List<Results> results = null;
            
            if(isBooleanOptionSet(argsLine, "oltp")){
                try {
                    if (isBooleanOptionSet(argsLine, "calibrate")) {
                        results = runOLTPWorkload(benchList, setup, verbose, intervalMonitor, true);
                    }
                    else{
                        results = runOLTPWorkload(benchList, setup, verbose, intervalMonitor,false);
                    }

                } catch (Throwable ex) {
                    LOG.error("Unexpected error when running OLTP benchmark.", ex);
                    System.exit(1);
                }
            }
            else if(isBooleanOptionSet(argsLine, "olap")){
                try {
                    if (isBooleanOptionSet(argsLine, "calibrate")) {
                        results = runOLAPWorkload(benchList, setup, verbose, intervalMonitor, true);
                    }
                    else{
                        results = runOLAPWorkload(benchList, setup, verbose, intervalMonitor,false);
                    }

                } catch (Throwable ex) {
                    LOG.error("Unexpected error when running OLAP benchmark.", ex);
                    System.exit(1);
                }
            }
            //Hybrid
            else{
                try {
                    if (isBooleanOptionSet(argsLine, "calibrate")) {
                        results = runHybridWorkload(benchList, setup, verbose, intervalMonitor, true,error_margin);
                    }
                    else{
                        results = runHybridWorkload(benchList, setup, verbose, intervalMonitor,false,error_margin);
                    }

                } catch (Throwable ex) {
                    LOG.error("Unexpected error when running Hybrid benchmark.", ex);
                    System.exit(1);
                }
            }    
                
            assert(results != null);

            PrintStream ps = System.out;
            PrintStream rs = System.out;
            
            // Special result uploader
            for(Results r : results){
            assert(r != null);    
            LOG.info(r.getClass());
            ResultUploader ru = new ResultUploader(r, xmlConfig, argsLine);

            if (argsLine.hasOption("o")) {
                // Check if directory needs to be created
                if (outputDirectory.length() > 0) {
                    FileUtil.makeDirIfNotExists(outputDirectory.split("/"));
                }
                
                // Build the complex path
                String baseFile = timestampValue + argsLine.getOptionValue("o");
                
                // Increment the filename for new results
                String nextName = FileUtil.getNextFilename(FileUtil.joinPath(outputDirectory, baseFile + ".res"));
                ps = new PrintStream(new File(nextName));
                LOG.info("Output into file: " + nextName);

                nextName = FileUtil.getNextFilename(FileUtil.joinPath(outputDirectory, baseFile + ".raw"));
                rs = new PrintStream(new File(nextName));
                LOG.info("Output Raw data into file: " + nextName);

                nextName = FileUtil.getNextFilename(FileUtil.joinPath(outputDirectory, baseFile + ".summary"));
                PrintStream ss = new PrintStream(new File(nextName));
                System.out.println("Our result uploader" + ru);
                LOG.info("Output summary data into file: " + nextName);
                if (ru != null) ru.writeSummary(ss);
                ss.close();

                nextName = FileUtil.getNextFilename(FileUtil.joinPath(outputDirectory, baseFile + ".db.cnf"));
                ss = new PrintStream(new File(nextName));
                LOG.info("Output db config into file: " + nextName);
                if (ru != null) ru.writeDBParameters(ss);
                ss.close();

                nextName = FileUtil.getNextFilename(FileUtil.joinPath(outputDirectory, baseFile + ".ben.cnf"));
                ss = new PrintStream(new File(nextName));
                LOG.info("Output benchmark config into file: " + nextName);
                if (ru != null) ru.writeBenchmarkConf(ss);
                ss.close();
            } else if (LOG.isDebugEnabled()) {
                LOG.debug("No output file specified");
            }
            
            if (argsLine.hasOption("s")) {
                int windowSize = Integer.parseInt(argsLine.getOptionValue("s"));
                LOG.info("Grouped into Buckets of " + windowSize + " seconds");
                r.writeCSV(windowSize, ps);

                if (isBooleanOptionSet(argsLine, "upload") && ru != null) {
                    ru.uploadResult();
                }

                // Allow more detailed reporting by transaction to make it easier to check
                if (argsLine.hasOption("ss")) {
                    
                    for (TransactionType t : activeTXTypes) {
                        PrintStream ts = ps;
                        
                        if (ts != System.out) {
                            // Get the actual filename for the output
                            String baseFile = timestampValue + argsLine.getOptionValue("o") + "_" + t.getName();
                            String prepended = outputDirectory + timestampValue;
                            String nextName = FileUtil.getNextFilename(FileUtil.joinPath(outputDirectory, baseFile + ".res"));                            
                            ts = new PrintStream(new File(nextName));
                            r.writeCSV(windowSize, ts, t);
                            ts.close();
                        }
                    }
                }
            } else if (LOG.isDebugEnabled()) {
                LOG.warn("No bucket size specified");
            }
            if (argsLine.hasOption("histograms")) {
                LOG.info(SINGLE_LINE);
                LOG.info("Completed Transactions:\n" + r.getTransactionSuccessHistogram() + "\n");
                LOG.info("Aborted Transactions:\n" + r.getTransactionAbortHistogram() + "\n");
                LOG.info("Rejected Transactions:\n" + r.getTransactionRetryHistogram());
                LOG.info("Unexpected Errors:\n" + r.getTransactionErrorHistogram());
                if (r.getTransactionAbortMessageHistogram().isEmpty() == false)
                    LOG.info("User Aborts:\n" + StringUtil.formatMaps(r.getTransactionAbortMessageHistogram()));
            } else if (LOG.isDebugEnabled()) {
                LOG.warn("No bucket size specified");
            }

            r.writeAllCSVAbsoluteTiming(rs);
            }
            ps.close();
            rs.close();
            
        } else {
            LOG.info("Skipping benchmark workload execution");
        }
    }

    private static List<String> get_weights(String plugin, SubnodeConfiguration work) {
            
            List<String> weight_strings = new LinkedList<String>();
            @SuppressWarnings("unchecked")
            List<SubnodeConfiguration> weights = work.configurationsAt("weights");
            boolean weights_started = false;
            
            for (SubnodeConfiguration weight : weights) {
                
                // stop if second attributed node encountered
                if (weights_started && weight.getRootNode().getAttributeCount() > 0) {
                    break;
                }
                //start adding node values, if node with attribute equal to current plugin encountered
                if (weight.getRootNode().getAttributeCount() > 0 && weight.getRootNode().getAttribute(0).getValue().equals(plugin)) {
                    weights_started = true;
                }
                if (weights_started) {
                    weight_strings.add(weight.getString(""));
                }
                
            }
            return weight_strings;
    }
    
    private static void runScript(BenchmarkModule bench, String script) {
        LOG.debug(String.format("Running %s", script));
        bench.runScript(script);
    }

    private static void runCreator(BenchmarkModule bench, boolean verbose) {
        LOG.debug(String.format("Creating %s Database", bench));
        bench.createDatabase();
    }
    
    private static void runLoader(BenchmarkModule bench, boolean verbose,boolean calibrate, boolean generateFiles, String filePath) {
        if(calibrate){
            LOG.debug(String.format("Loading %s Database for calibration procedure", bench));
            bench.loadDatabase(calibrate,generateFiles,filePath);
        }
        else{
            LOG.debug(String.format("Loading %s Database", bench));
            bench.loadDatabase(calibrate,generateFiles,filePath);
        }
    }

    private static List<Results> runHybridWorkload(List<BenchmarkModule> benchList, WorkloadSetup setup, boolean verbose, int intervalMonitor,boolean calibrate,double error_margin) throws QueueLimitException, IOException, InstantiationException, IllegalAccessException, InterruptedException {               
        BenchmarkModule bench = benchList.get(0);
        List<Worker> oltp_workers = new ArrayList<Worker>();
        List<Worker> olap_workers = new ArrayList<Worker>();
        
        List<WorkloadConfiguration> workConfs = new ArrayList<WorkloadConfiguration>();
        workConfs.add(bench.getWorkloadConfiguration());
        Clock clock = new Clock(workConfs.get(0).getTargetTPS(),(int)workConfs.get(0).getScaleFactor(),false);
        
        List<Results> results= new ArrayList<Results>();
         
        /**
         * HTPAB: THE CLIENT BALANCER IS INITIALIZED HERE:.
         */
        LOG.info("Creating CLIENT BALANCER");
        ClientBalancer clientBalancer = new ClientBalancer(bench,oltp_workers,olap_workers,bench.getWorkloadConfiguration(),clock,verbose,intervalMonitor,error_margin);
        Thread client_balancer = new Thread(clientBalancer);
        client_balancer.start();
        
        //***************************
        //      stream OLTP
        //***************************
        LOG.info("Creating " + bench.getWorkloadConfiguration().getTerminals() + " virtual terminals...");
        oltp_workers.addAll(bench.makeWorkers(verbose,"TPCC",clock));
        LOG.info(String.format("Launching the %s Benchmark with %s Phases...",
        bench.getBenchmarkName(), bench.getWorkloadConfiguration().getNumberOfPhases()));
            
        LOG.info("Started OLTP execution with "+bench.getWorkloadConfiguration().getTerminals()+" terminals.");
        LOG.info("Target TPS: "+ bench.getWorkloadConfiguration().getTargetTPS()+" TPS");
        
        OLTPWorkerThread oltp_runnable = new OLTPWorkerThread(oltp_workers, workConfs, intervalMonitor, calibrate);
        Thread oltp = new Thread(oltp_runnable);
        oltp.start();
        
        //***************************
        //      stream OLAP
        //***************************
        OLAPWorkerThread olap_runnable = new OLAPWorkerThread(olap_workers, workConfs, intervalMonitor, calibrate);
        Thread olap = new Thread(olap_runnable);
        olap.start();
        
        //***************************
        //      shutting down
        //***************************        
        oltp.join();
        olap.join();
        clientBalancer.terminate();
        client_balancer.join();
        
        //***************************
        //      COLLECT RESULTS
        //***************************
        //Thread.sleep(1000*60*2);
        Results tpcc = null;
        Results tpch = null;
        
        try{
            tpcc = oltp_runnable.getResults();
            tpch = olap_runnable.getResults();
        }
        catch(NullPointerException e){

        }
        
        boolean proceed = false;
        while(!proceed){
            
            if(tpcc != null || tpch != null){
                proceed = true;
                break;
            }
            else{
                try{
                    tpcc = oltp_runnable.getResults();
                    tpch = olap_runnable.getResults();
                }
                catch(NullPointerException e){
                    
                }
                LOG.info("[HTAPB Thread]: Still waiting for results from OLTP and OLAP workers. Going to sleep for 1 minute..." );
                Thread.sleep(60000);
            }
        }
        
        Results balancer = clientBalancer.getResults();
        
        //Thread.sleep(1000*60*2);

        results.add(tpcc);
        results.add(tpch);
        results.add(balancer);        
        
        return results;
    }

    private static List<Results> runOLTPWorkload(List<BenchmarkModule> benchList, WorkloadSetup setup, boolean verbose, int intervalMonitor,boolean calibrate) throws QueueLimitException, IOException, InstantiationException, IllegalAccessException, InterruptedException {               
        BenchmarkModule bench = benchList.get(0);
        List<Worker> oltp_workers = new ArrayList<Worker>();
        //List<Worker> olap_workers = new ArrayList<Worker>();
        
        List<WorkloadConfiguration> workConfs = new ArrayList<WorkloadConfiguration>();
        workConfs.add(bench.getWorkloadConfiguration());
        Clock clock = new Clock(workConfs.get(0).getTargetTPS(),(int)workConfs.get(0).getScaleFactor(),false);
        
        List<Results> results= new ArrayList<Results>();
        
        //***************************
        //      stream OLTP
        //***************************
        LOG.info("Creating " + bench.getWorkloadConfiguration().getTerminals() + " virtual terminals...");
        oltp_workers.addAll(bench.makeWorkers(verbose,"TPCC",clock));
        LOG.info(String.format("Launching the %s Benchmark with %s Phases...",
        bench.getBenchmarkName(), bench.getWorkloadConfiguration().getNumberOfPhases()));
            
        LOG.info("Started OLTP execution with "+bench.getWorkloadConfiguration().getTerminals()+" terminals.");
        LOG.info("Target TPS: "+ bench.getWorkloadConfiguration().getTargetTPS()+" TPS");
        
        OLTPWorkerThread oltp_runnable = new OLTPWorkerThread(oltp_workers, workConfs, intervalMonitor, calibrate);
        Thread oltp = new Thread(oltp_runnable);
        oltp.start();
             
        oltp.join();
        
        //***************************
        //      COLLECT RESULTS
        //***************************
        //Thread.sleep(1000*60*2);
        Results tpcc = null;
        
        try{
            tpcc = oltp_runnable.getResults();
        }
        catch(NullPointerException e){

        }
        
        boolean proceed = false;
        while(!proceed){
            
            if(tpcc != null){
                proceed = true;
                break;
            }
            else{
                try{
                    tpcc = oltp_runnable.getResults();
                }
                catch(NullPointerException e){
                    
                }
                LOG.info("[HTAPB Thread]: Still waiting for results from OLTP workers. Going to sleep for 1 minute..." );
                Thread.sleep(60000);
            }
        }    
        //Thread.sleep(1000*60*2);
        results.add(tpcc);     
       
        return results;
    }
    
    private static List<Results> runOLAPWorkload(List<BenchmarkModule> benchList, WorkloadSetup setup, boolean verbose, int intervalMonitor,boolean calibrate) throws QueueLimitException, IOException, InstantiationException, IllegalAccessException, InterruptedException {               
        BenchmarkModule bench = benchList.get(0);
        List<Worker> olap_workers = new ArrayList<Worker>();
        
        List<WorkloadConfiguration> workConfs = new ArrayList<WorkloadConfiguration>();
        workConfs.add(bench.getWorkloadConfiguration());
        Clock clock = new Clock(workConfs.get(0).getTargetTPS(),(int)workConfs.get(0).getScaleFactor(),false);
        
        List<Results> results= new ArrayList<Results>();
        
        //***************************
        //      stream OLAP
        //***************************
        LOG.info("Creating " + bench.getWorkloadConfiguration().getTerminals() + " virtual terminals...");
        olap_workers.addAll(bench.makeOLAPWorker(verbose, clock));
        LOG.info(String.format("Launching the %s Benchmark with %s Phases...",
        bench.getBenchmarkName(), bench.getWorkloadConfiguration().getNumberOfPhases()));
        
        LOG.info("Started OLAP execution with "+bench.getWorkloadConfiguration().getTerminals()+" terminals.");
        
        OLAPWorkerThread olap_runnable = new OLAPWorkerThread(olap_workers, workConfs, intervalMonitor, calibrate);
        Thread olap = new Thread(olap_runnable);
        olap.start();
        
        //***************************
        //      shutting down
        //***************************        
        olap.join();
        
        //***************************
        //      COLLECT RESULTS
        //***************************
        //Thread.sleep(1000*60*2);
        Results tpch = null;
        
        try{
            tpch = olap_runnable.getResults();
        }
        catch(NullPointerException e){

        }
        
        boolean proceed = false;
        while(!proceed){
            
            if(tpch != null){
                proceed = true;
                break;
            }
            else{
                try{
                    tpch = olap_runnable.getResults();
                }
                catch(NullPointerException e){
                    
                }
                LOG.info("[HTAPB Thread]: Still waiting for results from OLTP and OLAP workers. Going to sleep for 1 minute..." );
                Thread.sleep(60000);
            }
        }
        //Thread.sleep(1000*60*2);

        results.add(tpch);       
        
        return results;
    }
    
    private static void printUsage(Options options) {
        HelpFormatter hlpfrmt = new HelpFormatter();
        hlpfrmt.printHelp("oltpbenchmark", options);
    }

    /**
     * Returns true if the given key is in the CommandLine object and is set to
     * true.
     * 
     * @param argsLine
     * @param key
     * @return
     */
    private static boolean isBooleanOptionSet(CommandLine argsLine, String key) {
        if (argsLine.hasOption(key)) {
            LOG.debug("CommandLine has option '" + key + "'. Checking whether set to true");
            String val = argsLine.getOptionValue(key);
            LOG.debug(String.format("CommandLine %s => %s", key, val));
            return (val != null ? val.equalsIgnoreCase("true") : false);
        }
        return (false);
    }
    
}
