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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Handler to write to and read from auxiliar file.
 */
public class AuxiliarFileHandler {
    
    public static void writeToFile(String file_path, long start_ts, long final_ts){
        OutputStream out = null;
        try {
            Properties props = new Properties();
            props.setProperty("startLoadTimestamp", ""+start_ts);
            props.setProperty("lastLoadTimestamp", ""+final_ts);
            File file = new File(file_path+"htapb_auxiliar");
            out = new FileOutputStream(file);
            props.store(out, "HTAPB auxiliar file");
            
        } catch (FileNotFoundException ex) {
            Logger.getLogger(AuxiliarFileHandler.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(AuxiliarFileHandler.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            try {
                out.close();
            } catch (IOException ex) {
                Logger.getLogger(AuxiliarFileHandler.class.getName()).log(Level.SEVERE, null, ex);
            }
        }        
    }
    
    public static long importLastTs(String file_path){
        long result=0l;
        try {
            Properties props = new Properties();
            InputStream input = null;
            
            File file = new File(file_path+"htapb_auxiliar");
            input = new FileInputStream(file);
            
            if(input == null){
                input.getClass().getResourceAsStream(file_path+"htapb_auxiliar");
            }
            props.load(input);
            
            String res = props.getProperty("lastLoadTimestamp");
            result = Long.parseLong(res);
            
        } catch (FileNotFoundException ex) {
            Logger.getLogger(AuxiliarFileHandler.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(AuxiliarFileHandler.class.getName()).log(Level.SEVERE, null, ex);
        }
        return result;
    }
    
    public static long importFirstTs(String file_path){
        long result=0l;
        try {
            Properties props = new Properties();
            InputStream input = null;
            
            File file = new File(file_path+"htapb_auxiliar");
            input = new FileInputStream(file);
            
            if(input == null){
                input.getClass().getResourceAsStream(file_path+"htapb_auxiliar");
            }
            props.load(input);
            
            String res = props.getProperty("startLoadTimestamp");
            result = Long.parseLong(res);
            
        } catch (FileNotFoundException ex) {
            Logger.getLogger(AuxiliarFileHandler.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(AuxiliarFileHandler.class.getName()).log(Level.SEVERE, null, ex);
        }
        return result;
    }
    
    
   
    
}
