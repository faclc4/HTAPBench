
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

public class Q9 extends GenericQuery {
    
    private SQLStmt buildQueryStmt(){  
        RandomParameters random = new RandomParameters("uniform");
        String st1 = random.generateRandomCharacter();
        String st2 = random.generateRandomCharacter();
        String data=st1.toUpperCase()+st2.toUpperCase();
        data = "%"+data;
        
        String query = "SELECT n_name, "
            +        "sum(ol_amount) AS sum_profit "
            + "FROM "
            +  HTAPBConstants.TABLENAME_ITEM + ", "
            +  HTAPBConstants.TABLENAME_STOCK + ", "
            +  HTAPBConstants.TABLENAME_SUPPLIER + ", "
            +  HTAPBConstants.TABLENAME_ORDERLINE + ", "
            +  HTAPBConstants.TABLENAME_ORDER +  ", "
            +  HTAPBConstants.TABLENAME_NATION
            + " WHERE ol_i_id = s_i_id "
            +   "AND ol_supply_w_id = s_w_id "
            +   "AND ol_w_id = o_w_id "
            +   "AND ol_d_id = o_d_id "
            +   "AND ol_o_id = o_id "
            +   "AND ol_i_id = i_id "
            +   "AND su_nationkey = n_nationkey "
            +   "AND i_data LIKE '"+data+"' "
            + "GROUP BY n_name "
            + "ORDER BY n_name "
                + "DESC";
        return new SQLStmt(query);
    }
	
    @Override
    protected SQLStmt get_query(Clock clock,WorkloadConfiguration wrklConf) {
        return buildQueryStmt();
    }
}