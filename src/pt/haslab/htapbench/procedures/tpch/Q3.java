
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

public class Q3 extends GenericQuery {
    
    private SQLStmt buildQueryStmt(Clock clock){ 
        RandomParameters random = new RandomParameters("uniform");
        
        int day = RandomParameters.randBetween(1, 31);
        long date = RandomParameters.convertDatetoLong(1995, 3, day);
        Timestamp ts = new Timestamp(clock.transformTsFromSpecToLong(date));
        
        String state = random.generateRandomCharacter();
        state=state.toUpperCase();
        state = state+"%";
        
        String query ="SELECT ol_o_id, "
            +        "ol_w_id, "
            +        "ol_d_id, "
            +        "sum(ol_amount) AS revenue, "
            +        "o_entry_d "
            + "FROM "
            +  HTAPBConstants.TABLENAME_CUSTOMER + ", "
            +  HTAPBConstants.TABLENAME_NEWORDER + ", "
            +  HTAPBConstants.TABLENAME_ORDER +    ", "
            +  HTAPBConstants.TABLENAME_ORDERLINE
            +  " WHERE c_state LIKE '"+state+"' "
            +   "AND c_id = o_c_id "
            +   "AND c_w_id = o_w_id "
            +   "AND c_d_id = o_d_id "
            +   "AND no_w_id = o_w_id "
            +   "AND no_d_id = o_d_id "
            +   "AND no_o_id = o_id "
            +   "AND ol_w_id = o_w_id "
            +   "AND ol_d_id = o_d_id "
            +   "AND ol_o_id = o_id "
            +   "AND o_entry_d > '"+ts.toString()+"' "
            + "GROUP BY ol_o_id, "
            +          "ol_w_id, "
            +          "ol_d_id, "
            +          "o_entry_d "
            + "ORDER BY revenue DESC , o_entry_d";
        return new SQLStmt(query);
    }

    @Override
    protected SQLStmt get_query(Clock clock,WorkloadConfiguration wrklConf) {
        return buildQueryStmt(clock);
    }
}