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

public class Q17 extends GenericQuery {
	
    private SQLStmt buildQueryStmt(){  
        RandomParameters random = new RandomParameters("uniform");
        String st1 = "%"+random.generateRandomCharacter();
        
        String query = "SELECT SUM(ol_amount) / 2.0 AS avg_yearly "
            + "FROM "+ HTAPBConstants.TABLENAME_ORDERLINE + ", "
            +   "(SELECT i_id, AVG (ol_quantity) AS a "
            +    "FROM "
            +    HTAPBConstants.TABLENAME_ITEM + ", "
            +    HTAPBConstants.TABLENAME_ORDERLINE
            +    " WHERE i_data LIKE '"+st1+"' "
            +      "AND ol_i_id = i_id "
            +    "GROUP BY i_id) t "
            + "WHERE ol_i_id = t.i_id "
            +   "AND ol_quantity < t.a";
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