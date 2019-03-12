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
package pt.haslab.htapbench.random;

import java.sql.Timestamp;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.configuration.tree.xpath.XPathExpressionEngine;
import pt.haslab.htapbench.core.WorkloadSetup;
import pt.haslab.htapbench.densitity.Clock;
import pt.haslab.htapbench.densitity.DensityConsultant;
import pt.haslab.htapbench.exceptions.HTAPBException;

public class RandomExecutor {
    
    
    
    public static void main (String[]args) throws ConfigurationException, HTAPBException{
        // Initialize log4j
        String log4jPath = System.getProperty("log4j.configuration");
        if (log4jPath != null) {
            org.apache.log4j.PropertyConfigurator.configure(log4jPath);
        } else {
            throw new RuntimeException("Missing log4j.properties file");
        }
        
        String configFile = "config/htapb_config_postgres.xml";
        XMLConfiguration xmlConfig = new XMLConfiguration(configFile);
        xmlConfig.setExpressionEngine(new XPathExpressionEngine());
        
        
        WorkloadSetup setup = new WorkloadSetup(xmlConfig);
        setup.computeWorkloadSetup();
        
        
        DensityConsultant density = new DensityConsultant(10000);
        System.out.println("Density: "+density.getDensity());
        System.out.println("Delta TS: "+density.getDeltaTs());
        System.out.println("Target TPS "+density.getTargetTPS());
        
        long deltaTs = density.getDeltaTs();
        
        Clock clock = new Clock(deltaTs,false);
        
        System.out.println("Clock: current TS "+clock.getCurrentTs());

        int year = RandomParameters.randBetween(1993, 1997);
        int month = RandomParameters.randBetween(1, 12);
        long date1 = RandomParameters.convertDatetoLong(year, month, 1);
        long date2 = RandomParameters.convertDatetoLong(year+1, month, 1);
        Timestamp ts1 = new Timestamp(clock.transformTsFromSpecToLong(date1));  
        Timestamp ts2 = new Timestamp(clock.transformTsFromSpecToLong(date2));
        
        System.out.println(ts1.toString());
        System.out.println(ts2.toString());
        
    }
    
}

