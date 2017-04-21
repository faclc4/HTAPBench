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
package pt.haslab.htapbench.procedures.tpch;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.log4j.Logger;
import pt.haslab.htapbench.api.Procedure;
import pt.haslab.htapbench.api.SQLStmt;
import pt.haslab.htapbench.api.Worker;
import pt.haslab.htapbench.core.WorkloadConfiguration;
import pt.haslab.htapbench.densitity.Clock;

public abstract class GenericQuery extends Procedure {

    protected static final Logger LOG = Logger.getLogger(GenericQuery.class);

    private PreparedStatement stmt;
    private Connection conn;
    private Worker owner;

    public void setOwner(Worker w) {
        this.owner = w;
    }

    protected static SQLStmt initSQLStmt(String queryFile) {
        StringBuilder query = new StringBuilder();

        try{
            FileReader input = new FileReader("src/eu/leanbigdata/htapb/benchmark/procedures/tpch/" + queryFile);
            BufferedReader reader = new BufferedReader(input);
            String line = reader.readLine();
            while (line != null) {
                query.append(line);
                query.append(" ");
                line = reader.readLine();
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new SQLStmt(query.toString());
    }

    protected abstract SQLStmt get_query(Clock clock,WorkloadConfiguration wrklConf);

    public int run(Connection conn,Clock clock, WorkloadConfiguration wrklConf) throws SQLException {
        //initializing all prepared statements
        stmt = this.getPreparedStatement(conn, get_query(clock,wrklConf));
        
        LOG.debug("Query SQL STMT:"+ get_query(clock,wrklConf).getSQL());
        
        if (owner != null)
            owner.setCurrStatement(stmt);

        //LOG.debug(this.getClass());
        LOG.info(this.getClass());
        ResultSet rs = null;
        try {            
            rs = stmt.executeQuery();
        } catch(SQLException ex) {
            // If the system thinks we're missing a prepared statement, then we
            // should regenerate them.
            if (ex.getErrorCode() == 0 && ex.getSQLState() != null
                && ex.getSQLState().equals("07003"))
            {
                this.resetPreparedStatements();
                rs = stmt.executeQuery();
            }
            else {
                throw ex;
            }
        }
        int t = 0;
        while (rs.next()) {
            t = t+1;
        }

        if (owner != null)
            owner.setCurrStatement(null);
        
        return t;

    }
}
