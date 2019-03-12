
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
package pt.haslab.htapbench.procedures.tpch;

import pt.haslab.htapbench.core.WorkloadConfiguration;
import pt.haslab.htapbench.api.SQLStmt;
import pt.haslab.htapbench.benchmark.HTAPBConstants;
import pt.haslab.htapbench.densitity.Clock;
import pt.haslab.htapbench.random.RandomParameters;
import java.sql.Timestamp;

public class Q12 extends GenericQuery {
    
    private SQLStmt buildQueryStmt(Clock clock){  
        int year = RandomParameters.randBetween(1993, 1997);
        long date1 = RandomParameters.convertDatetoLong(year, 1, 1);
        long date2 = RandomParameters.convertDatetoLong(year+1, 1, 1);
        Timestamp ts1 = new Timestamp(clock.transformTsFromSpecToLong(date1));  
        Timestamp ts2 = new Timestamp(clock.transformTsFromSpecToLong(date2));
        
        
        String query = "SELECT o_ol_cnt, "
            +        "sum(CASE WHEN o_carrier_id = 1 "
            +            "OR o_carrier_id = 2 THEN 1 ELSE 0 END) AS high_line_count, "
            +        "sum(CASE WHEN o_carrier_id <> 1 "
            +            "AND o_carrier_id <> 2 THEN 1 ELSE 0 END) AS low_line_count "
            + "FROM "
            + HTAPBConstants.TABLENAME_ORDER + ", "
            + HTAPBConstants.TABLENAME_ORDERLINE
            + " WHERE ol_w_id = o_w_id "
            +   "AND ol_d_id = o_d_id "
            +   "AND ol_o_id = o_id "
            +   "AND o_entry_d <= ol_delivery_d "
            +   "AND ol_delivery_d >= '"+ts1.toString()+"' "
            +   "AND ol_delivery_d < '"+ts2.toString()+"' "
            + "GROUP BY o_ol_cnt "
            + "ORDER BY o_ol_cnt";
        return new SQLStmt(query);
    }
	
    @Override
    protected SQLStmt get_query(Clock clock,WorkloadConfiguration wrklConf) {
        return buildQueryStmt(clock);
    }
}