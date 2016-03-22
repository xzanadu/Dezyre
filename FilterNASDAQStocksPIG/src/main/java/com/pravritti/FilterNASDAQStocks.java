package com.pravritti;

import org.apache.pig.FilterFunc;
import org.apache.pig.data.Tuple;

import java.io.IOException;

/**
 * Created by Srinivas Bhagavathula on 3/21/16.
 */
public class FilterNASDAQStocks extends FilterFunc {
    @Override
    public Boolean exec(Tuple tuple) throws IOException {
        if (tuple == null || tuple.size() == 0) {
            return false;
        }
        try {
            Object object1 = tuple.get(0);
            Object object2 = tuple.get(1);
            Object object3 = tuple.get(2);
            Object object4 = tuple.get(3);
            if (object1 == null || object2 == null || object3 == null || object4 == null)
                return false;

            Double spHigh = (Double) object2;
            Double spLow = (Double) object3;
            Double spOpen = (Double) object1;
            Double spClose = (Double) object4;

            if (spOpen > spHigh || spClose > spLow)
                return false;
            return true;
        } catch(Exception e){
            return false;}
    }
}
