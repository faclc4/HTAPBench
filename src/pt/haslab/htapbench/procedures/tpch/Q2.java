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

public class Q2 extends GenericQuery {
    
    private SQLStmt buildQueryStmt(){  
        RandomParameters random = new RandomParameters("uniform");
        String region = random.getRandomRegion();
        region = region.substring(0, region.length()-1)+"%";
        String i_data = "%"+random.generateRandomCharacter();
        String query ="SELECT su_suppkey, "
            +        "su_name, "
            +        "n_name, "
            +        "i_id, "
            +        "i_name, "
            +        "su_address, "
            +        "su_phone, "
            +        "su_comment "
            + "FROM "
            + HTAPBConstants.TABLENAME_ITEM + ", "
            + HTAPBConstants.TABLENAME_SUPPLIER + ", "
            + HTAPBConstants.TABLENAME_STOCK + ", "
            + HTAPBConstants.TABLENAME_NATION +", " 
            + HTAPBConstants.TABLENAME_REGION +", "
            +   "(SELECT s_i_id AS m_i_id, MIN(s_quantity) AS m_s_quantity "
            +    "FROM "
            + HTAPBConstants.TABLENAME_STOCK +     ", "
            + HTAPBConstants.TABLENAME_SUPPLIER +  ", "
            + HTAPBConstants.TABLENAME_NATION +    ", "
            + HTAPBConstants.TABLENAME_REGION
            +      " WHERE su_nationkey=n_nationkey "
            +      "AND n_regionkey=r_regionkey "
            +      "AND r_name LIKE '"+region+"' "
            +    "GROUP BY s_i_id) m "
            + "WHERE i_id = s_i_id "
            +   "AND su_nationkey = n_nationkey "
            +   "AND n_regionkey = r_regionkey "
            +   "AND i_data LIKE '"+i_data+"' "
            +   "AND r_name LIKE '"+region+"' "
            +   "AND i_id=m_i_id "
            +   "AND s_quantity = m_s_quantity "
            + "ORDER BY n_name, "
            +          "su_name, "
            +          "i_id";
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