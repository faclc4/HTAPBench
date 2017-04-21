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
package pt.haslab.htapbench.densitity;

/**
 * This class houses the density specification for a standard mix execution of TPC-C with think time.
 */
public class DensityConsultant {
    
    private final double m = 1.26956;
    private final double b = 0.0103497;
    private double density;
    //deltaTs in ms.
    private long deltaTS;
    private final int targetTPS;
    
    
    public DensityConsultant(int targetTPS){
        this.targetTPS=targetTPS;
        this.computeDensity();
        this.computeDeltaTS();
    }
    
    /**
     * Computes the density found during the TPC-C execute stage. This value is pre-defines for the standard TPC-X Txn mix.
     */
    private void computeDensity(){
        this.density= m*targetTPS +b;
    }
    
    /**
     * Return how many seconds should the clock move forward at each new TS issue process.
     */
    private void computeDeltaTS(){
        this.deltaTS=(long)(1000/density);
    }
    
    /**
     * Returns the computed Density.
     * @return 
     */
    public double getDensity(){
        return this.density;
    }
    
    /**
     * Computes the Timestamp Delta used at each clock tick.
     * @return 
     */
    public long getDeltaTs(){
        return this.deltaTS;
    }    
    
    /**
     * Return the TargetTPS considered for the delta computation.
     * @return 
     */
    public int getTargetTPS(){
        return this.targetTPS;
    }
}
