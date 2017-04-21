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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 *
 * @author Fábio Coelho
 */
public final class TimeBucketIterator implements Iterator<DistributionStatistics> {
        private final Iterator<LatencyRecord.Sample> samples;
        private final int windowSizeSeconds;
        private final TransactionType txType;

        private LatencyRecord.Sample sample;
        private long nextStartNs;

        private DistributionStatistics next;

        /**
         * @param samples
         * @param windowSizeSeconds
         * @param txType
         *            Allows to filter transactions by type
         */
        public TimeBucketIterator(Iterator<LatencyRecord.Sample> samples, int windowSizeSeconds, TransactionType txType) {
            this.samples = samples;
            this.windowSizeSeconds = windowSizeSeconds;
            this.txType = txType;

            if (samples.hasNext()) {
                sample = samples.next();
                // TODO: To be totally correct, we would want this to be the
                // timestamp of the start
                // of the measurement interval. In most cases this won't matter.
                nextStartNs = sample.startNs;
                calculateNext();
            }
        }

        private void calculateNext() {
            assert next == null;
            assert sample != null;
            assert sample.startNs >= nextStartNs;

            // Collect all samples in the time window
            ArrayList<Integer> latencies = new ArrayList<Integer>();
            long endNs = nextStartNs + windowSizeSeconds * 1000000000L;
            while (sample != null && sample.startNs < endNs) {

                // Check if a TX Type filter is set, in the default case,
                // INVALID TXType means all should be reported, if a filter is
                // set, only this specific transaction
                if (txType == TransactionType.INVALID || txType.getId() == sample.tranType)
                    latencies.add(sample.latencyUs);

                if (samples.hasNext()) {
                    sample = samples.next();
                } else {
                    sample = null;
                }
            }

            // Set up the next time window
            assert sample == null || endNs <= sample.startNs;
            nextStartNs = endNs;

            int[] l = new int[latencies.size()];
            for (int i = 0; i < l.length; ++i) {
                l[i] = latencies.get(i);
            }

            next = DistributionStatistics.computeStatistics(l);
        }

        @Override
        public boolean hasNext() {
            return next != null;
        }

        @Override
        public DistributionStatistics next() {
            if (next == null)
                throw new NoSuchElementException();
            DistributionStatistics out = next;
            next = null;
            if (sample != null) {
                calculateNext();
            }
            return out;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("unsupported");
        }
    }
