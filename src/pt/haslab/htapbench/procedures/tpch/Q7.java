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

public class Q7 extends GenericQuery {
    
    private SQLStmt buildQueryStmt(Clock clock){  
        RandomParameters random = new RandomParameters("uniform");
        String nation1 = random.getRandomNation();
        String nation2 = random.getRandomNation();
        
        long date1 = RandomParameters.convertDatetoLong(1995, 1, 1);
        long date2 = RandomParameters.convertDatetoLong(1996, 12, 31);
        Timestamp ts1 = new Timestamp(clock.transformTsFromSpecToLong(date1));  
        Timestamp ts2 = new Timestamp(clock.transformTsFromSpecToLong(date2));        
        
        String query = "SELECT su_nationkey AS supp_nation, "
            +        "sum(ol_amount) AS revenue "
            + "FROM "
            + HTAPBConstants.TABLENAME_SUPPLIER+  ", "
            + HTAPBConstants.TABLENAME_STOCK +    ", "
            + HTAPBConstants.TABLENAME_ORDERLINE+ ", "
            + HTAPBConstants.TABLENAME_ORDER +    ", "
            + HTAPBConstants.TABLENAME_CUSTOMER + ", "
            + HTAPBConstants.TABLENAME_NATION   + " n1, "
            + HTAPBConstants.TABLENAME_NATION   + " n2 "
            + "WHERE ol_supply_w_id = s_w_id "
            +   "AND ol_i_id = s_i_id "
            +   "AND ol_w_id = o_w_id "
            +   "AND ol_d_id = o_d_id "
            +   "AND ol_o_id = o_id "
            +   "AND c_id = o_c_id "
            +   "AND c_w_id = o_w_id "
            +   "AND c_d_id = o_d_id "
            +   "AND su_nationkey = n1.n_nationkey "
            +   "AND substr(c_state,1,1) = substr(n2.n_name,1,1) "
            +   "AND ((n1.n_name = '"+nation1+"' "
            +         "AND n2.n_name = '"+nation2+"') "
            +        "OR (n1.n_name = '"+nation2+"' "
            +            "AND n2.n_name = '"+nation1+"')) "
            +            "AND ol_delivery_d between '"+ts1.toString()+"' and '"+ts2.toString()
            + "' GROUP BY su_nationkey "
            + "ORDER BY su_nationkey, "
            ;
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