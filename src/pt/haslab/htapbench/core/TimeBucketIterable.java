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
package pt.haslab.htapbench.core;

import pt.haslab.htapbench.api.TransactionType;
import java.util.Iterator;

public final class TimeBucketIterable implements Iterable<DistributionStatistics> {
        private final Iterable<LatencyRecord.Sample> samples;
        private final int windowSizeSeconds;
        private final TransactionType txType;

        /**
         * @param samples
         * @param windowSizeSeconds
         * @param txType
         *            Allows to filter transactions by type
         */
        public TimeBucketIterable(Iterable<LatencyRecord.Sample> samples, int windowSizeSeconds, TransactionType txType) {
            this.samples = samples;
            this.windowSizeSeconds = windowSizeSeconds;
            this.txType = txType;
        }

        @Override
        public Iterator<DistributionStatistics> iterator() {
            return new TimeBucketIterator(samples.iterator(), windowSizeSeconds, txType);
        }
    }
