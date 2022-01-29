package babel.demos.utils;

import java.math.BigInteger;

public class ID {

    // arg can be eather an ID or Topic
    public static final BigInteger getID(short m, int next){
     
        return new BigInteger("2").pow(next - 1).abs().mod(new BigInteger(Integer.toString(m)));
    }

    public static final BigInteger getID(short m, int next, BigInteger base){
     
        return base.add(new BigInteger("2").pow(next - 1).abs()).mod(new BigInteger("2").pow(m));
    }
    
}
