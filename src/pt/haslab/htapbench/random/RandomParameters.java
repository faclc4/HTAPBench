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
package pt.haslab.htapbench.random;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Random;
import pt.haslab.htapbench.distributions.Distribution;
import pt.haslab.htapbench.distributions.HotspotDistribution;
import pt.haslab.htapbench.distributions.UniformDistribution;


/**
 * This class presents a Random Parameter Generator.
 * This is class is used both for feeding the populate stage of the benchmark and whenever random parameters are needed to build random queries.
 * The parameters are used to bound the values generated.
 */
public class RandomParameters {
    
    public static int year_min = 2013;
    public static int year_max = 2015;
    public static int month_min = 1;
    public static int month_max = 12;
    public static int day_min = 1;
    public static int day_max = 30;
    public static int hour_min = 1;
    public static int hour_max = 24;
    public static int minute_min = 1;
    public static int minute_max = 60;
    public static int second_min = 1;
    public static int second_max = 60;
    
    
    private static List<String> nations = Arrays.asList("Australia", "Belgium", "Camaroon", "Denmark", "Ecuador","France","Germany","Hungary","Italy","Japan","Kenya","Lithuania","Mexico",
                                                       "Netherlands","Oman","Portugal","Qatar","Rwanda","Serbia","Togo","United States","Vietman","Singapore","Cambodia","Yemen","Zimbabwe",
                                                       "Argentina","Bolivia","Canada","Dominican Republic","Egypt","Finnland","Ghana","Haiti","India","Jamaica","kazahkstan","Luxembourg","Morocco",
                                                       "Norway","Poland","Peru","Nicaragua","Romania","South Africa","Thailand","United Kingdom","Venezuela","Liechtenstei","Austria","Laos","Zambia",
                                                       "Switzerland","China","Papua New Guinea","East Timor","Bulgaria","Brazil","Albania","Andorra","Belize","Botswana");
    private static List<String> regions = Arrays.asList("Africa","America","Asia","Australia","Europe");
    private static List<Character> alphabet = Arrays.asList('a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z');
    private static List<String> su_comment = Arrays.asList("good","bad");
    
    private String distributionType = null;
    
    public RandomParameters(String distributionType){
        this.distributionType=distributionType;
    }

    public RandomParameters() {
    }
    
   
    public Date generateRandomDateTime(){
        Date date = new Date();
        int year = randBetween(year_min-1900, year_max-1900);
        int month = randBetween(month_min, month_max);
        int day = randBetween(day_min, day_max);
        int hour = randBetween(hour_min, hour_max);
        int minute = randBetween(minute_min, minute_max);
        int second = randBetween(second_min, second_max);
        
        date.setYear(year);
        date.setMonth(month);
        date.setDate(day);
        date.setHours(hour);
        date.setMinutes(minute);
        date.setSeconds(second);
       
        return date;
    }
    
    public static long convertDatetoLong(int year,int month, int day){
        Timestamp ts = new Timestamp(year,month,day,0,0,0,0);        
        return ts.getTime();
    }
    
    public static long addMonthsToDate(long ts,int months){
        Date date = new Date(ts);
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.add(Calendar.MONTH, months);
        return cal.getTime().getTime();
    }
    
    public long generateRandoDateTime(){
        Date aux = generateRandomDateTime();
        return aux.getTime();
    }
    
    public String generateRandomCharacter(){
        Distribution dist = getDistributionType(alphabet.size()-1);
        int rand = dist.nextInt();
        return ""+alphabet.get(rand);
    }
    
    
    public static int randBetween(int start, int end) {
        return start + (int)Math.round(Math.random() * (end - start));
    }
    
    public static double randDoubleBetween(int start, int end){
        return start + Math.random() * (end-start);
    }
    
    public String getRandomNation(){
        Distribution dist = getDistributionType(nations.size()-1);
        int rand = dist.nextInt();
        return nations.get(rand);
    }
    
    public String getRandomRegion(){
        Distribution dist = getDistributionType(regions.size()-1);
        int rand = dist.nextInt();
        return regions.get(rand);
    }
    
    public String getRandomSuComment(){
        Distribution dist = getDistributionType(su_comment.size());
        int rand = dist.nextInt();
        return su_comment.get(rand);
    }
    
    public String getRandomPhoneCountryCode(){
        Distribution dist = getDistributionType(nations.size()-1);
        int rand = dist.nextInt() +10;
        return ""+rand;
    }
    
    public Distribution getDistributionType(int size){
        Distribution dist = null;
        int rand = 0;
        if(distributionType.equals("uniform")){
             dist = new UniformDistribution(1,size-1);
        }
        if(distributionType.equals("hotspot")){
            int lower_bound = 1;
            int upper_bound = size-1;
            double hotsetFraction = 0.5;
            double hotOpnFraction = 0.5;
            dist = new HotspotDistribution(lower_bound, upper_bound, hotsetFraction, hotOpnFraction);
        }
        return dist;
    }
    
    public int randInteger(int max){
        Distribution dist = getDistributionType(max);
        return dist.nextInt();
    }
    
    /**
     * The exponetial distribution used by the TPC, for instance to calulate the
     * transaction's thinktime.
     * 
     * @param rand
     *            the random generator
     * @param min
     *            the minimum number which could be accepted for this
     *            distribution
     * @param max
     *            the maximum number which could be accept for this distribution
     * @param lMin
     *            the minimum number which could be accept for the following
     *            execution rand.nextDouble
     * @param lMax
     *            the maximum number which could be accept for the following
     *            execution rand.nexDouble
     * @param mu
     *            the base value provided to calculate the exponetial number.
     *            For instance, it could be the mean thinktime
     * @return the caluclated exponetial number
     */
    public static final long negExp(Random rand, long min, double lMin,
                    long max, double lMax, double mu) {
            double r = rand.nextDouble();

            if (r < lMax) {
                    return (max);
            }
            return ((long) (-mu * Math.log(r)));

    }
}
