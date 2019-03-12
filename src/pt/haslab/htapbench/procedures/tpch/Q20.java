
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

public class Q20 extends GenericQuery {
    
    private SQLStmt buildQueryStmt(Clock clock){ 
        RandomParameters random = new RandomParameters("uniform");
        String nation = random.getRandomNation();
        String char1 = random.generateRandomCharacter()+"%";
        
        int year = RandomParameters.randBetween(1993, 1997);
        int month = RandomParameters.randBetween(1, 12);
        long date1 = RandomParameters.convertDatetoLong(year, month, 1);
        long date2 = RandomParameters.convertDatetoLong(year+1, month, 1);
        Timestamp ts1 = new Timestamp(clock.transformTsFromSpecToLong(date1));  
        Timestamp ts2 = new Timestamp(clock.transformTsFromSpecToLong(date2));
        
        
        String query = "SELECT su_name, "
            +        "su_address "
            + "FROM "
            + HTAPBConstants.TABLENAME_SUPPLIER + ", "
            + HTAPBConstants.TABLENAME_NATION
            + " WHERE su_suppkey IN "
            +     "(SELECT mod(s_i_id * s_w_id, 10000) "
            +      "FROM " + HTAPBConstants.TABLENAME_STOCK
            +      " INNER JOIN "+ HTAPBConstants.TABLENAME_ITEM+" ON i_id = s_i_id "
            +      "INNER JOIN "+HTAPBConstants.TABLENAME_ORDERLINE+" ON ol_i_id = s_i_id "
            +      "WHERE ol_delivery_d >= '"+ts1.toString()+"' "
            +      "AND ol_delivery_d < '"+ts2.toString()+"' "
            +        "AND i_data LIKE '"+char1+"' "
            +      "GROUP BY s_i_id, "
            +               "s_w_id, "
            +               "s_quantity HAVING 2*s_quantity > sum(ol_quantity)) "
            +   "AND su_nationkey = n_nationkey "
            +   "AND n_name = '"+nation+"' "
            + "ORDER BY su_name";
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