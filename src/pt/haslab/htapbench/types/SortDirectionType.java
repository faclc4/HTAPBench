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
package pt.haslab.htapbench.types;

import java.util.*;

/**
 *
 */
public enum SortDirectionType {
    INVALID      (0),
    ASC          (1),
    DESC         (2);

    SortDirectionType(int val) {
        assert (this.ordinal() == val) :
            "Enum element " + this.name() +
            " in position " + this.ordinal() +
            " instead of position " + val;
    }

    public int getValue() {
        return this.ordinal();
    }

    protected static final Map<Integer, SortDirectionType> idx_lookup = new HashMap<Integer, SortDirectionType>();
    protected static final Map<String, SortDirectionType> name_lookup = new HashMap<String, SortDirectionType>();
    static {
        for (SortDirectionType vt : EnumSet.allOf(SortDirectionType.class)) {
            SortDirectionType.idx_lookup.put(vt.ordinal(), vt);
            SortDirectionType.name_lookup.put(vt.name().toLowerCase().intern(), vt);
        }
    }

    public static Map<Integer, SortDirectionType> getIndexMap() {
        return idx_lookup;
    }

    public static Map<String, SortDirectionType> getNameMap() {
        return name_lookup;
    }

    public static SortDirectionType get(Integer idx) {
        assert(idx >= 0);
        SortDirectionType ret = SortDirectionType.idx_lookup.get(idx);
        return (ret == null ? SortDirectionType.INVALID : ret);
    }

    public static SortDirectionType get(String name) {
        SortDirectionType ret = SortDirectionType.name_lookup.get(name.toLowerCase().intern());
        return (ret == null ? SortDirectionType.INVALID : ret);
    }
}
