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
import java.sql.Timestamp;

public class Q8 extends GenericQuery {
	
    private SQLStmt buildQueryStmt(Clock clock){  
        RandomParameters random = new RandomParameters("uniform");
        String nation1 = random.getRandomNation();
        String region = random.getRandomRegion();
        String i_data = "%"+random.generateRandomCharacter();
        String quantity = ""+RandomParameters.randBetween(1, 1000);
        
        long date1 = RandomParameters.convertDatetoLong(1995, 1, 1);
        long date2 = RandomParameters.convertDatetoLong(1996, 12, 31);
        Timestamp ts1 = new Timestamp(clock.transformTsFromSpecToLong(date1));  
        Timestamp ts2 = new Timestamp(clock.transformTsFromSpecToLong(date2));
                
        String query = "SELECT sum(CASE WHEN n2.n_name = '"+nation1+"' THEN ol_amount ELSE 0 END) / sum(ol_amount) AS mkt_share "
                + "FROM "
                + HTAPBConstants.TABLENAME_ITEM +", "
                + HTAPBConstants.TABLENAME_SUPPLIER +", "
                + HTAPBConstants.TABLENAME_STOCK + ", "
                + HTAPBConstants.TABLENAME_ORDERLINE +", "
                + HTAPBConstants.TABLENAME_ORDER + ", "
                + HTAPBConstants.TABLENAME_CUSTOMER + ", "
                + HTAPBConstants.TABLENAME_NATION + " n1, "
                + HTAPBConstants.TABLENAME_NATION + " n2, "
                + HTAPBConstants.TABLENAME_REGION + " "
                + "WHERE "
                    + "i_id = s_i_id AND "
                    + "ol_i_id = s_i_id AND "
                    + "ol_supply_w_id = s_w_id AND "
                    + "ol_w_id = o_w_id AND "
                    + "ol_d_id = o_d_id AND "
                    + "ol_o_id = o_id AND "
                    + "c_id = o_c_id AND "
                    + "c_w_id = o_w_id AND "
                    + "c_d_id = o_d_id AND "
                    + "substring(n1.n_name,1,1) = substring(c_state,1,1) AND "
                    + "n1.n_regionkey = r_regionkey AND "
                    + "ol_i_id < "+quantity+" AND "
                    + "r_name = '"+region+"' AND "
                    + "su_nationkey = n2.n_nationkey AND "
                    + "o_entry_d between '"+ts1.toString()+"' AND '"+ts2.toString()+"' "
                    + "AND i_data LIKE '"+i_data+"' AND "
                    + "i_id = ol_i_id";
        return new SQLStmt(query);
    }
    
    @Override
    protected SQLStmt get_query(Clock clock,WorkloadConfiguration wrklConf) {
        return buildQueryStmt(clock);
    }
}