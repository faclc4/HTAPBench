
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
import java.sql.Connection;
import java.sql.Statement;
import java.sql.SQLException;
import java.sql.Timestamp;

public class Q15 extends GenericQuery {
	
    private SQLStmt buildViewStmt(Clock clock){  
        int year = RandomParameters.randBetween(1993, 1997);
        int month=0;
        if(year == 1997)
            month = RandomParameters.randBetween(1, 10);
        else 
            month = RandomParameters.randBetween(1, 12);
        long date1 = RandomParameters.convertDatetoLong(year, month, 1);
        long date2 = RandomParameters.addMonthsToDate(date1, 3);
        
        Timestamp ts1 = new Timestamp(clock.transformTsFromSpecToLong(date1));  
        Timestamp ts2 = new Timestamp(clock.transformTsFromSpecToLong(date2));
        
        String view = "CREATE view revenue0 (supplier_no, total_revenue) AS "
            +     "SELECT "
            +         "mod((s_w_id * s_i_id),10000) as supplier_no, "
            +         "sum(ol_amount) as total_revenue "
            +     "FROM "
            +     HTAPBConstants.TABLENAME_ORDERLINE +    ",  "
            +     HTAPBConstants.TABLENAME_STOCK
            +     " WHERE "
            +         "ol_i_id = s_i_id "
            +         "AND ol_supply_w_id = s_w_id "
            +         "AND ol_delivery_d between '"+ts1.toString()+"' AND '"+ts2.toString()
            +         "' GROUP BY "
            +         "supplier_no";
        return new SQLStmt(view);
    }
    
    private SQLStmt buildQueryStmt(){  
        String query = "SELECT su_suppkey, "
            +        "su_name, "
            +        "su_address, "
            +        "su_phone, "
            +        "total_revenue "
            + "FROM "
            + HTAPBConstants.TABLENAME_SUPPLIER +", revenue0 "
            + "WHERE su_suppkey = supplier_no "
            +     "AND total_revenue = (select max(total_revenue) from revenue0) "
            + "ORDER BY su_suppkey";
        return new SQLStmt(query);
    }
		
    public final SQLStmt dropview_stmt = new SQLStmt(
              "DROP VIEW revenue0"
        );
	
    @Override
    protected SQLStmt get_query(Clock clock,WorkloadConfiguration wrklConf) {      
        return buildQueryStmt();
    }

    public int run(Connection conn,Clock clock,WorkloadConfiguration wrklConf) throws SQLException {
        // With this query, we have to set up a view before we execute the
        // query, then drop it once we're done.
        Statement stmt = conn.createStatement();
        int ret = 0;
        try {
            stmt.executeUpdate(buildViewStmt(clock).getSQL());
            ret = super.run(conn,clock,wrklConf);
        } finally {
            stmt.executeUpdate(dropview_stmt.getSQL());
        }

        return ret;
    }
}