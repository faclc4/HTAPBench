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

public class Q5 extends GenericQuery {
	
    private SQLStmt buildQueryStmt(Clock clock){  
        RandomParameters random = new RandomParameters("uniform");
        String region = random.getRandomRegion();
        
        int year = RandomParameters.randBetween(1993, 1997);
        long date1 = RandomParameters.convertDatetoLong(year, 1, 1);
        long date2 = RandomParameters.convertDatetoLong(year+1, 1, 1);
        Timestamp ts1 = new Timestamp(clock.transformTsFromSpecToLong(date1));  
        Timestamp ts2 = new Timestamp(clock.transformTsFromSpecToLong(date2));
        
        String query = "SELECT n_name, "
            +        "sum(ol_amount) AS revenue "
            + "FROM "
            +  HTAPBConstants.TABLENAME_CUSTOMER + ", "
            +  HTAPBConstants.TABLENAME_ORDER +    ", "
            +  HTAPBConstants.TABLENAME_ORDERLINE+ ", "
            +  HTAPBConstants.TABLENAME_STOCK +    ", "
            +  HTAPBConstants.TABLENAME_SUPPLIER + ", "
            +  HTAPBConstants.TABLENAME_NATION +   ", "
            +  HTAPBConstants.TABLENAME_REGION 
            + " WHERE c_id = o_c_id "
            +   "AND c_w_id = o_w_id "
            +   "AND c_d_id = o_d_id "
            +   "AND ol_o_id = o_id "
            +   "AND ol_w_id = o_w_id "
            +   "AND ol_d_id=o_d_id "
            +   "AND ol_w_id = s_w_id "
            +   "AND ol_i_id = s_i_id "
            +   "AND su_nationkey = n_nationkey "
            +   "AND n_regionkey = r_regionkey "
            +   "AND r_name = '"+region+"' "
            +   "AND o_entry_d >= '"+ts1.toString()+"' "
            +   "AND o_entry_d < '"+ts2.toString()+"' "     
            + "GROUP BY n_name "
            + "ORDER BY revenue DESC";
        return new SQLStmt(query);
    }

    /**
     *
     * @param clock
     * @param wrklConf
     * @return
     */
    @Override
    protected SQLStmt get_query(Clock clock,WorkloadConfiguration wrklConf) {
        return buildQueryStmt(clock);
    }
}