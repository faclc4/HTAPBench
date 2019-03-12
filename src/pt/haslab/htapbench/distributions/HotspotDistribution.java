
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
package pt.haslab.htapbench.distributions;

import java.util.Random;

public class HotspotDistribution extends Distribution {
    private final int lowerBound;
    private final int upperBound;
    private final int hotInterval;
    private final int coldInterval;
    private final double hotsetFraction;
    private final double hotOpnFraction;
  
  /**
   * Create a generator for Hotspot distributions.
   * 
   * @param lowerBound lower bound of the distribution.
   * @param upperBound upper bound of the distribution.
   * @param hotsetFraction percentage of data item
   * @param hotOpnFraction percentage of operations accessing the hot set.
   */
    public HotspotDistribution(int lowerBound, int upperBound, double hotsetFraction, double hotOpnFraction) {
        super("hotspot");
        if (hotsetFraction < 0.0 || hotsetFraction > 1.0) {
          System.err.println("Hotset fraction out of range. Setting to 0.0");
          hotsetFraction = 0.0;
        }
        if (hotOpnFraction < 0.0 || hotOpnFraction > 1.0) {
          System.err.println("Hot operation fraction out of range. Setting to 0.0");
          hotOpnFraction = 0.0;
        }
        if (lowerBound > upperBound) {
          System.err.println("Upper bound of Hotspot generator smaller than the lower bound. " +
                    "Swapping the values.");
          int temp = lowerBound;
          lowerBound = upperBound;
          upperBound = temp;
        }
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
        this.hotsetFraction = hotsetFraction;
        int interval = upperBound - lowerBound + 1;
        this.hotInterval = (int)(interval * hotsetFraction);
        this.coldInterval = interval - hotInterval;
        this.hotOpnFraction = hotOpnFraction;
    }

    @Override
    public int nextInt() {
        int value = 0;
        Random random = Utils.random();
        if (random.nextDouble() < hotOpnFraction) {
          // Choose a value from the hot set.
          value = lowerBound + random.nextInt(hotInterval);
        } else {
          // Choose a value from the cold set.
          value = lowerBound + hotInterval + random.nextInt(coldInterval);
        }
        return value;
    }

      /**
       * @return the lowerBound
       */
    public int getLowerBound() {
        return lowerBound;
    }

      /**
       * @return the upperBound
       */
    public int getUpperBound() {
        return upperBound;
    }

      /**
       * @return the hotsetFraction
       */
    public double getHotsetFraction() {
        return hotsetFraction;
    }

      /**
       * @return the hotOpnFraction
       */
    public double getHotOpnFraction() {
        return hotOpnFraction;
    }
    
    public double mean() {
        return hotOpnFraction * (lowerBound + hotInterval/2.0)
          + (1 - hotOpnFraction) * (lowerBound + hotInterval + coldInterval/2.0);
    }
}
