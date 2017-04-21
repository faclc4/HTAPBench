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

public class Q19 extends GenericQuery {
    
    private SQLStmt buildQueryStmt(){  
        RandomParameters random = new RandomParameters("uniform");
        int price1 = RandomParameters.randBetween(1, 10);
        int price12 = price1+10;
        int price2 = RandomParameters.randBetween(10, 20);
        int price22 = price2+10;
        int price3 = RandomParameters.randBetween(20, 30);
        int price32 = price3+10;
        String char1 = "%"+random.generateRandomCharacter();
        String char2 = "%"+random.generateRandomCharacter();
        String char3 = "%"+random.generateRandomCharacter();
        
        String query = "SELECT sum(ol_amount) AS revenue "
            + "FROM "
            + HTAPBConstants.TABLENAME_ORDERLINE + ", "
            + HTAPBConstants.TABLENAME_ITEM
            + " WHERE (ol_i_id = i_id "
            +        "AND i_data LIKE '"+char1+"' "
            +        "AND ol_quantity >="+ 1
            +        " AND ol_quantity <="+ 5
            +        " AND i_price BETWEEN "+price1+" AND "+ price12
            +        " AND ol_w_id IN (1, "
            +                        "2, "
            +                        "3)) "
            +   "OR (ol_i_id = i_id "
            +       "AND i_data LIKE '"+char2+"' "
            +       "AND ol_quantity >="+ 1
            +       " AND ol_quantity <= "+ 10
            +       " AND i_price BETWEEN "+price2+" AND " + price22
            +       " AND ol_w_id IN (1, "
            +                       "2, "
            +                       "4)) "
            +   "OR (ol_i_id = i_id "
            +       "AND i_data LIKE '"+char3+"' "
            +       "AND ol_quantity >="+ 1
            +       " AND ol_quantity <= "+ 15
            +       " AND i_price BETWEEN "+price3+" AND "+price32
            +       " AND ol_w_id IN (1, "
            +                       "5, "
            +                       "3))";
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