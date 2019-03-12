
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

public class Q16 extends GenericQuery {    
    
    private SQLStmt buildQueryStmt(){  
        RandomParameters random = new RandomParameters("uniform");
        String st1 = random.generateRandomCharacter();
        String st2 = random.generateRandomCharacter();
        String data = st1+st2+"%";
        String su_comment = "%"+random.getRandomSuComment()+"%";
        
        
        String query = "SELECT i_name, "
            +        "substring(i_data from  1 for 3) AS brand, "
            +        "i_price, "
            +        "count(DISTINCT (mod((s_w_id * s_i_id),10000))) AS supplier_cnt "
            + "FROM "
            + HTAPBConstants.TABLENAME_STOCK + ", "
            + HTAPBConstants.TABLENAME_ITEM
            + " WHERE i_id = s_i_id "
            +   "AND i_data NOT LIKE '"+data+"' "
            +   "AND (mod((s_w_id * s_i_id),10000) NOT IN "
            +     "(SELECT su_suppkey "
            +      "FROM "+HTAPBConstants.TABLENAME_SUPPLIER
            +      " WHERE su_comment LIKE '"+su_comment+"')) "
            + "GROUP BY i_name, "
            +          "brand, "
            +          "i_price "
            + "ORDER BY supplier_cnt DESC";
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