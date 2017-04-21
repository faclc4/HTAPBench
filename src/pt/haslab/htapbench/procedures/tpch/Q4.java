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

public class Q4 extends GenericQuery {
    
    private SQLStmt buildQueryStmt(Clock clock){
        int year = RandomParameters.randBetween(1993, 1997);
        int month=0;
        if(year == 1997)
            month = RandomParameters.randBetween(1, 10);
        else 
            month = RandomParameters.randBetween(1, 12);
        long date1 = RandomParameters.convertDatetoLong(year, month, 1);
        long date2 = RandomParameters.addMonthsToDate(date1, 3);
        
        Timestamp ts1 = new Timestamp(clock.transformTsFromSpecToLong(date1));  
        Timestamp ts2 = new Timestamp(clock.transformTsFromSpecToLong(date2));
        
        String query ="SELECT o_ol_cnt, "
            +        "count(*) AS order_count "
            + "FROM "
            + HTAPBConstants.TABLENAME_ORDER+" WHERE "
                + "o_entry_d >= '"
                + ts1.toString()
                + "' and o_entry_d < '"
                + ts2.toString()
                + "' and exists "
            +     "(SELECT * "
            +      "FROM "
            +       HTAPBConstants.TABLENAME_ORDERLINE + " WHERE o_id = ol_o_id "
            +        "AND o_w_id = ol_w_id "
            +        "AND o_d_id = ol_d_id "
            +        "AND ol_delivery_d >= o_entry_d) "
            + "GROUP BY o_ol_cnt "
            + "ORDER BY o_ol_cnt";
        return new SQLStmt(query);
    }

    @Override
    protected SQLStmt get_query(Clock clock,WorkloadConfiguration wrklConf) {
        return buildQueryStmt(clock);
    }
}