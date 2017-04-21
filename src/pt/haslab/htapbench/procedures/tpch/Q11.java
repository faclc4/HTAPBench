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
import java.text.DecimalFormat;

public class Q11 extends GenericQuery {
    
    private SQLStmt buildQueryStmt(WorkloadConfiguration wrklConf){  
        RandomParameters random = new RandomParameters("uniform");
        String nation1 = random.getRandomNation();
        
       
        double frac = 0.0001/wrklConf.getScaleFactor();
        String fraction = ""+new DecimalFormat("#.#####").format(frac);
        
        String query = "SELECT s_i_id, "
            +        "sum(s_order_cnt) AS ordercount "
            + "FROM "
            + HTAPBConstants.TABLENAME_STOCK + ", "
            + HTAPBConstants.TABLENAME_SUPPLIER + ", "
            + HTAPBConstants.TABLENAME_NATION
            + " WHERE "
            +   "su_nationkey = n_nationkey "
            +   "AND n_name = '"+nation1+"' "
            + "GROUP BY s_i_id HAVING sum(s_order_cnt) > "
            +   "(SELECT sum(s_order_cnt) * "+fraction+" "
            +    "FROM "
            +    HTAPBConstants.TABLENAME_STOCK + ", "
            +    HTAPBConstants.TABLENAME_SUPPLIER + ", "
            +    HTAPBConstants.TABLENAME_NATION
            +    " WHERE "
            +      "su_nationkey = n_nationkey "
            +      "AND n_name = '"+nation1+"') "
            + "ORDER BY ordercount DESC";
        return new SQLStmt(query);
    }
	
    @Override
    protected SQLStmt get_query(Clock clock,WorkloadConfiguration wrklConf) {
        return buildQueryStmt(wrklConf);
    }
}