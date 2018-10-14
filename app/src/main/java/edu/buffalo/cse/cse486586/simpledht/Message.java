package edu.buffalo.cse.cse486586.simpledht;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by utsav on 4/13/2018.
 */

public class Message implements Serializable {
    //1 joining nodes
    //2 updating live nodes
    //3 insertion
    //4 deletion
    //5 query
    //6 reply to query
    //7 query everything
    //8 reply to query evrything
    //9 deletion everything

    public String msgType= null;
    public String senderNodeID = null;
    public ArrayList<String> nodesList = new ArrayList<String>();
    public HashMap ContentValues = new HashMap();
    public String successor = null;//nodeid
    public String predecessor = null;//nodeid
    public String typeofquery = null;
    public String senderofrepplyingall = null;
}
