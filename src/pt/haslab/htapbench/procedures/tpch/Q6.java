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

import pt.haslab.htapbench.core.WorkloadConfiguration;
import pt.haslab.htapbench.api.SQLStmt;
import pt.haslab.htapbench.benchmark.HTAPBConstants;
import pt.haslab.htapbench.densitity.Clock;
import pt.haslab.htapbench.random.RandomParameters;
import java.sql.Timestamp;

public class Q6 extends GenericQuery {
    
    private SQLStmt buildQueryStmt(Clock clock){  
        String q1 = ""+RandomParameters.randBetween(1, 100000);
        String q2 = ""+RandomParameters.randBetween(1, 100000);
        int year = RandomParameters.randBetween(1993, 1997);
        long date1 = RandomParameters.convertDatetoLong(year, 1, 1);
        long date2 = RandomParameters.convertDatetoLong(year+1, 1, 1);
        Timestamp ts1 = new Timestamp(clock.transformTsFromSpecToLong(date1));  
        Timestamp ts2 = new Timestamp(clock.transformTsFromSpecToLong(date2));
        
        String query = "SELECT sum(ol_amount) AS revenue "
            + "FROM  "+HTAPBConstants.TABLENAME_ORDERLINE
            + " WHERE ol_delivery_d >= '"+ts1+"' "
            +   "AND ol_delivery_d < '"+ts2+"' "
            +   "AND ol_quantity BETWEEN "+q1+" AND "+q2;
        return new SQLStmt(query);
    }
	
    @Override
    protected SQLStmt get_query(Clock clock,WorkloadConfiguration wrklConf) {
        return buildQueryStmt(clock);
    }
}