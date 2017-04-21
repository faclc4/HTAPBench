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

public class Q18 extends GenericQuery {
    
    private SQLStmt buildQueryStmt(){  
        String amount = ""+ RandomParameters.randDoubleBetween(312, 315);
        
        String query = "SELECT c_last, "
            +        "c_id, "
            +        "o_id, "
            +        "o_entry_d, "
            +        "o_ol_cnt, "
            +        "sum(ol_amount) AS amount_sum "
            + "FROM "
            + HTAPBConstants.TABLENAME_CUSTOMER + ", "
            + HTAPBConstants.TABLENAME_ORDER +    ", "
            + HTAPBConstants.TABLENAME_ORDERLINE
            + " WHERE c_id = o_c_id "
            +   "AND c_w_id = o_w_id "
            +   "AND c_d_id = o_d_id "
            +   "AND ol_w_id = o_w_id "
            +   "AND ol_d_id = o_d_id "
            +   "AND ol_o_id = o_id "
            + "GROUP BY o_id, "
            +          "o_w_id, "
            +          "o_d_id, "
            +          "c_id, "
            +          "c_last, "
            +          "o_entry_d, "
            +          "o_ol_cnt HAVING sum(ol_amount) > "+amount+" "
            + "ORDER BY amount_sum DESC, o_entry_d";
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