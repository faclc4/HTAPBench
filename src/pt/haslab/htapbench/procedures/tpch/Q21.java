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

public class Q21 extends GenericQuery {
	
    private SQLStmt buildQueryStmt(){  
        RandomParameters random = new RandomParameters("uniform");
        String nation = random.getRandomNation();
        
        String query = "SELECT su_name, "
            +        "count(*) AS numwait "
            + "FROM "
            + HTAPBConstants.TABLENAME_SUPPLIER + ", "
            + HTAPBConstants.TABLENAME_ORDERLINE +" l1, "
            + HTAPBConstants.TABLENAME_ORDER + ", "
            + HTAPBConstants.TABLENAME_STOCK + ", "
            + HTAPBConstants.TABLENAME_NATION 
            + " WHERE ol_o_id = o_id "
            +   "AND ol_w_id = o_w_id "
            +   "AND ol_d_id = o_d_id "
            +   "AND ol_w_id = s_w_id "
            +   "AND ol_i_id = s_i_id "
            +   "AND l1.ol_delivery_d > o_entry_d "
            +   "AND NOT EXISTS "
            +     "(SELECT * "
            +      "FROM "+HTAPBConstants.TABLENAME_ORDERLINE+" l2 "
            +      "WHERE l2.ol_o_id = l1.ol_o_id "
            +        "AND l2.ol_w_id = l1.ol_w_id "
            +        "AND l2.ol_d_id = l1.ol_d_id "
            +        "AND l2.ol_delivery_d > l1.ol_delivery_d) "
            +   "AND su_nationkey = n_nationkey "
            +   "AND n_name = '"+nation+"' "
            + "GROUP BY su_name "
            + "ORDER BY numwait DESC, su_name";
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
        return buildQueryStmt();
    }
}