
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

public class Q13 extends GenericQuery {
    
    private SQLStmt buildQueryStmt(){  
        String id = ""+RandomParameters.randBetween(1, 10);
        
        String query = "SELECT c_count, "
            +        "count(*) AS custdist "
            + "FROM "
            +   "(SELECT c_id, "
            +           "count(o_id) AS c_count "
            +    "FROM " + HTAPBConstants.TABLENAME_CUSTOMER
            +    " LEFT OUTER JOIN "+HTAPBConstants.TABLENAME_ORDER+" ON (c_w_id = o_w_id "
            +                               "AND c_d_id = o_d_id "
            +                               "AND c_id = o_c_id "
            +                               "AND o_carrier_id > "+id+") "
            +    "GROUP BY c_id) AS c_orders "
            + "GROUP BY c_count "
            + "ORDER BY custdist DESC, c_count DESC";
        return new SQLStmt(query);
    }	
    @Override
    protected SQLStmt get_query(Clock clock,WorkloadConfiguration wrklConf) {
        return buildQueryStmt();
    }
}