package babel.demos.utils;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Hash {

    // arg can be eather an ID or Topic
    public static final BigInteger getHash(String arg){
        try{
            MessageDigest digest = MessageDigest.getInstance("SHA-256");

            byte[] messageDigest = digest.digest(arg.getBytes());

            // // Convert byte array into signum representation 
            BigInteger id = new BigInteger(messageDigest).abs();

            return id;

        }catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static final BigInteger getHash2(String arg){
        try{

            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.reset();
            digest.update(arg.getBytes());
            return new BigInteger(digest.digest()).abs();

        }catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return null;
    }
}
