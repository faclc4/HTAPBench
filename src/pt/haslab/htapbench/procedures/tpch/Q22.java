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

public class Q22 extends GenericQuery {
    
    private SQLStmt buildQueryStmt(){  
        RandomParameters random = new RandomParameters("uniform");
        String code1 = random.getRandomPhoneCountryCode();
        String code2 = random.getRandomPhoneCountryCode();
        String code3 = random.getRandomPhoneCountryCode();
        String code4 = random.getRandomPhoneCountryCode();
        String code5 = random.getRandomPhoneCountryCode();
        String code6 = random.getRandomPhoneCountryCode();
        String code7 = random.getRandomPhoneCountryCode();
        
        String query = "SELECT substring(c_state from 1 for 1) AS country, "
            + "count(*) AS numcust, "
            + "sum(c_balance) AS totacctbal "
            + "FROM " +HTAPBConstants.TABLENAME_CUSTOMER 
            + " WHERE substring(c_phone from 1 for 2) IN ('"+code1+"', "
            +                                    "'"+code2+"', "
            +                                    "'"+code3+"', "
            +                                    "'"+code4+"', "
            +                                    "'"+code5+"', "
            +                                    "'"+code6+"', "
            +                                    "'"+code7+"') "
            +   "AND c_balance > "
            +     "(SELECT avg(c_balance) "
            +      "FROM "
            +      HTAPBConstants.TABLENAME_CUSTOMER
            +      " WHERE c_balance > 0.00 "
            +      "AND substring(c_phone from 1 for 2) IN ('"+code1+"',"
            +                                              "'"+code2+"',"
            +                                              "'"+code3+"',"
            +                                              "'"+code4+"',"
            +                                              "'"+code5+"',"
            +                                              "'"+code6+"',"
            +                                              "'"+code7+"')) "
            +   "AND NOT EXISTS "
            +     "(SELECT * "
            +      "FROM "
            +      HTAPBConstants.TABLENAME_ORDER 
            +      " WHERE o_c_id = c_id "
            +        "AND o_w_id = c_w_id "
            +        "AND o_d_id = c_d_id) "
            + "GROUP BY substring(c_state from 1 for 1) "
            + "ORDER BY substring(c_state,1,1)";
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