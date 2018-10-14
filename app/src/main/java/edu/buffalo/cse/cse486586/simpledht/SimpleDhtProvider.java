package edu.buffalo.cse.cse486586.simpledht;


import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Map;
import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;


public class SimpleDhtProvider extends ContentProvider {


    static final int SERVER_PORT = 10000;
    private static final String[] Ports = {"11108", "11112", "11116", "11120", "11124"};
    private static final String[] Device_ID = {"5554", "5556", "5558", "5560", "5562"};

    static String myNodeId = null;
    static String myPort = null;
    static String myNodeIdHash = null;
    static String prednode = null;
    static String succnode= null;

    private static ArrayList<String> nodesList = new ArrayList<String>();
    private HashMap<String,String> SenderHashMap = new HashMap();
    private HashMap<String, String> MapQuery= new HashMap<String, String>();
    private HashMap<String,HashMap<String,String>> MapQueryAll = new HashMap<String,HashMap<String,String>>();
    private Integer CountQueryAll= 1;



    public Uri myUri = null;

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        if( selection.equals("@") ) {

            Log.i("my", "Delete(@): start");
            try {
                String Allfiles[] = getContext().fileList();
                File directory = getContext().getFilesDir();
                for(int i = 0 ; i<Allfiles.length ; i++) {
                    File filetodelete = new File(directory, Allfiles[i]);
                    filetodelete.delete();
                }
            } catch (Exception e) {
                Log.e("my", "Delete(@): File delete failed");
                e.printStackTrace();
            }
            if(nodesList.size() ==0 || (nodesList.size() ==1 && myNodeId.equals("5554"))) {
                return 1;
            }

            String msgType = null;
            if(selectionArgs !=null) {
                msgType = selectionArgs[0];
            } else {
                return 1;
            }


            String initialQuerynode = selectionArgs[1];
            if(msgType.equals("9") && !SenderHashMap.get(succnode).equals(initialQuerynode)) {
                Message msgtosend = new Message();
                msgtosend.msgType="9";
                msgtosend.senderNodeID=initialQuerynode;
                msgtosend.successor=SenderHashMap.get(succnode);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgtosend);

            }

            Log.i("my","delete(@): end");
            return 1;

        } else if (selection.equals("*")) {
            Log.i("my","delete(*)");
            return 1;

        } else {
            Log.i("my","delete(direct): start");
            String filekey = selection;
            String filekeyhash= null;

            try {
                filekeyhash= genHash(filekey);
            } catch(NoSuchAlgorithmException e ) {
                Log.e("my", "Delete(direct): Gen hash exception:"+e.toString());
                e.printStackTrace();
            }

            if(nodesList.size() ==0 || (nodesList.size() ==1 && myNodeId.equals("5554"))) {
                try {
                    File directory = getContext().getFilesDir();
                    File filetodelete = new File(directory, filekey);
                    filetodelete.delete();
                    return 1;

                } catch (Exception e) {
                    Log.e("my", "Delete(direct): File delete failed");
                    e.printStackTrace();
                }
            }

            if(((filekeyhash.compareTo(prednode)>0) && (filekeyhash.compareTo(myNodeIdHash)<=0)) ||
                    ((filekeyhash.compareTo(prednode)>0) && nodesList.indexOf(myNodeIdHash)==0) ||
                    ((filekeyhash.compareTo(myNodeIdHash)<=0) && nodesList.indexOf(myNodeIdHash)==0)) {
                Log.i("my", "Delete(Direct): File at this node. deleting it");
                try {
                    File directory = getContext().getFilesDir();
                    File filetodelete = new File(directory, filekey);
                    filetodelete.delete();
                    return 1;

                } catch (Exception e) {
                    Log.e("my", "Delte(Direct): File delete failed");
                    e.printStackTrace();
                }
            } else {
                Log.i("my", "Delete(Direct): file not at this node");
                Message msgtosend = new Message();
                msgtosend.msgType="4";
                msgtosend.typeofquery=filekey;
                msgtosend.senderNodeID=myNodeId;
                msgtosend.successor=SenderHashMap.get(succnode);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgtosend);
            }
            Log.i("my","delete(direct): end");
            return 1;
        }
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        Log.i("my","insert(): start");
        String key = (String)values.get("key");
        String value = (String)values.get("value");
        String Keyhash = null;
        Log.i("my", "Insert(): key: "+key+":value: "+value);

        try {
            Keyhash  = genHash(key);
        } catch(NoSuchAlgorithmException e ) {
            Log.e("my", "Insert(): Gen hash exception:"+e.toString());
            e.printStackTrace();
        }

        if(nodesList.size() ==0 || (nodesList.size() ==1 && myNodeId.equals("5554"))) {
            try {
                FileOutputStream fileoutputStream= getContext().openFileOutput(key, Context.MODE_PRIVATE);
                fileoutputStream.write(value.getBytes());
                fileoutputStream.flush();
                fileoutputStream.close();

            } catch (Exception e) {
                Log.e("my", "Insert(): write failed");
            }
            Log.i("Insert(): successfull", values.toString());
            return uri;
        }

        if(((Keyhash.compareTo(prednode)>0) && (Keyhash.compareTo(myNodeIdHash)<=0)) ||
                ((Keyhash.compareTo(prednode)>0) && nodesList.indexOf(myNodeIdHash)==0) ||
                ((Keyhash.compareTo(myNodeIdHash)<=0) && nodesList.indexOf(myNodeIdHash)==0)) {
            try {
                FileOutputStream fileoutputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
                fileoutputStream.write(value.getBytes());
                fileoutputStream.flush();
                fileoutputStream.close();
            } catch (Exception e) {
                Log.e("my", "Insert(): write failed");
            }
            Log.i("Insert(): successfull", values.toString());
            return uri;
        } else {
            Message msgtosend = new Message();
            msgtosend.msgType="3";
            msgtosend.ContentValues.put("key",key);
            msgtosend.ContentValues.put("value",value);
            msgtosend.senderNodeID=myNodeId;
            msgtosend.successor=SenderHashMap.get(succnode);
            msgtosend.predecessor=SenderHashMap.get(prednode);

            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgtosend);
            Log.i("my", "Insert(): send msg to succ: "+msgtosend);
        }
        Log.i("my","insert(): end");
        return uri;
    }

    @Override
    public boolean onCreate() {
        Log.i("my","OnCreate(): start");
        TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);

        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority("edu.buffalo.cse.cse486586.simpledht.provider");
        uriBuilder.scheme("content");
        myUri = uriBuilder.build();

        myNodeId = portStr;
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        try {
            myNodeIdHash = genHash(portStr);
        } catch (NoSuchAlgorithmException e) {
            Log.e("my", "OnCreate(): gen hash exception:"+e.toString());
            e.printStackTrace();
        }

        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e("my", "OnCreate():server socket creation exception:"+e.toString());
            return  false;
        }

        for ( int i = 0 ; i<Device_ID.length ; i++) {

            try {
                String genHashVal = genHash(Device_ID[i]);
                SenderHashMap.put(genHashVal, Device_ID[i]);

            } catch(NoSuchAlgorithmException e ) {
                Log.e("my", "Insert():Gen hash exception:"+e.toString());
                e.printStackTrace();
            }
        }

        if(myNodeId.equals("5554")){
            Log.d("my", "OnCreate():First Node to join: Node id" + myNodeId  + "myNodeIdgenHash : " + myNodeIdHash);
            nodesList.add(myNodeIdHash);
            int myNodeIndex = nodesList.indexOf(myNodeIdHash);
            prednode = myNodeIdHash;
            succnode = myNodeIdHash;
        }
        else{
            Log.d("my", "OnCreate():Join from : Node id" + myNodeId);
            Message msgtosend = new Message();
            msgtosend.msgType="1";
            msgtosend.senderNodeID=myNodeId;
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgtosend);
        }
        Log.i("my","OnCreate(): end");
        return true;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        if( selection.equals("@") ) {
            Log.i("my","query(@): start");
            MatrixCursor matrixcursor = new MatrixCursor(new String[] { "key", "value"});
            int offset=0;
            String allFiles[] = getContext().fileList();
            for ( int cnt = 0 ; cnt < allFiles.length ; cnt++) {
                String tempfileName = allFiles[cnt];
                StringBuffer fileData = new StringBuffer("");
                try {
                    FileInputStream inputstream = getContext().openFileInput(tempfileName);
                    int noOfBytesRead=0;
                    byte[] buffer = new byte[1024];
                    while ((noOfBytesRead=inputstream.read(buffer)) != -1)
                    {
                        fileData.append(new String(buffer, offset, noOfBytesRead));
                    }
                } catch (Exception e) {
                    Log.e("my", "Query(@):reading file Exception: "+e.toString());
                }
                matrixcursor.addRow(new String[] {tempfileName, fileData.toString() });
            }

            if(nodesList.size() ==0 || (nodesList.size() ==1 && myNodeId.equals("5554"))) {
                return matrixcursor;
            }

            String msgType = null;
            String senderNodeId = null;

            if(projection!=null) {
                msgType = projection[0];
                senderNodeId = projection[1];
            }

            if(senderNodeId!=null && msgType.equals("7") && !SenderHashMap.get(succnode).equals(senderNodeId)) {
                Message msgtosend = new Message();
                msgtosend.msgType="7";
                msgtosend.senderofrepplyingall=myNodeId;
                msgtosend.senderNodeID= senderNodeId;
                msgtosend.successor=SenderHashMap.get(succnode);
                msgtosend.typeofquery="@";
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgtosend);
                Log.i("my", "Query(@):msg  send to Client task:" + msgtosend.toString());
            }
            Log.i("my","query(@):result count : "+matrixcursor.getCount());
            Log.i("my","query(@): end");
            return matrixcursor;

        } else if (selection.equals("*")) {
            Log.i("my","query(*): start");
            MatrixCursor matrixCursor = new MatrixCursor(new String[] { "key", "value"});
            String allFiles[] = getContext().fileList();

            for ( int counter = 0 ; counter < allFiles.length; counter++) {
                String tempfileName = allFiles[counter];
                int noOfBytesRead=0;
                int offset=0;
                StringBuffer fileData = new StringBuffer("");

                try {
                    FileInputStream inputStream = getContext().openFileInput(tempfileName);
                    byte[] buffer = new byte[1024];
                    while ((noOfBytesRead=inputStream.read(buffer)) != -1)
                    {
                        fileData.append(new String(buffer, offset, noOfBytesRead));
                    }
                } catch (Exception e) {
                    Log.e("my", "Query(*):file reading exception:"+e.toString());
                }
                matrixCursor.addRow(new String[] {tempfileName, fileData.toString() });
            }


            if(nodesList.size() ==0 || (nodesList.size() ==1 && myNodeId.equals("5554"))) {
                return matrixCursor;
            }


            String senderNodeId = null;
            if(projection!=null) {
                senderNodeId = projection[0];
            }

            if(senderNodeId == null ) {
                for(int i=0;i<Device_ID.length;i++) {
                    if(!myNodeId.equals(Device_ID[i])) {
                        MapQueryAll.put(Device_ID[i],null);
                    }
                }
            }

            Message msgtosend = new Message();
            msgtosend.msgType="7";
            if(senderNodeId  == null) {
                msgtosend.senderNodeID=myNodeId;
            } else {
                msgtosend.senderNodeID=senderNodeId;
            }
            msgtosend.successor=SenderHashMap.get(succnode);
            msgtosend.typeofquery="@";
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgtosend);
            Log.i("my", "Query(*): msg send to Client task: " + msgtosend.toString());

            Integer RepliesExpected = nodesList.size();

            if(senderNodeId==null) {

                while(CountQueryAll < RepliesExpected ){
                    //wait for other nodes result
                }

                for(Map.Entry<String,HashMap<String,String>> entry:MapQueryAll.entrySet())
                {
                    String emulatorID = (String)entry.getKey();
                    HashMap<String,String> fileMap = entry.getValue();
                    if(fileMap!=null)
                    {
                        for(Map.Entry<String,String> entry2:fileMap.entrySet())
                        {
                            String fileKey=(String)entry2.getKey();
                            String fileValue=(String)entry2.getValue();
                            matrixCursor.addRow(new String[]{fileKey, fileValue});
                        }
                    }
                }
            }
            Log.i("my","query(*): end");
            return matrixCursor;
        } else {
            Log.i("my","query(direct): start");
            String filekey = selection;
            String filekeyHash=null;
            int offset=0;

            try {
                filekeyHash = genHash(filekey);

            } catch(NoSuchAlgorithmException e ) {
                Log.e("my", "Query(Direct): gen hash exception:"+e.toString());
                e.printStackTrace();
            }

            StringBuffer fileData = new StringBuffer("");

            if(nodesList.size() ==0 || (nodesList.size() ==1 && myNodeId.equals("5554"))) {
                try {
                    FileInputStream inputstream = getContext().openFileInput(filekey);
                    int noOfBytesRead=0;
                    byte[] buffer = new byte[1024];
                    while ((noOfBytesRead=inputstream.read(buffer)) != -1)
                    {
                        fileData.append(new String(buffer, offset, noOfBytesRead));
                    }
                } catch (Exception e) {
                    Log.e("my", "Query(Direct): file reading exception"+e.toString());
                }
                MatrixCursor mc = new MatrixCursor(new String[] { "key", "value"});
                mc.addRow(new String[] {filekey, fileData.toString() });
                return mc;
            }

            if(((filekeyHash.compareTo(prednode)>0) && (filekeyHash.compareTo(myNodeIdHash)<=0)) ||
                    ((filekeyHash.compareTo(prednode)>0) && nodesList.indexOf(myNodeIdHash)==0) ||
                    ((filekeyHash.compareTo(myNodeIdHash)<=0) && nodesList.indexOf(myNodeIdHash)==0)) {
                try {
                    FileInputStream inputstream = getContext().openFileInput(filekey);
                    int noOfBytesRead=0;
                    byte[] buffer = new byte[1024];
                    while ((noOfBytesRead=inputstream.read(buffer)) != -1)
                    {
                        fileData.append(new String(buffer, offset, noOfBytesRead));
                    }
                } catch (Exception e) {
                    Log.e("my", "Query(direct): file reading exception:"+e.toString());
                }

            } else {
                String senderNodeId = null;
                if(projection!=null) {
                    senderNodeId = projection[0];
                }

                if(senderNodeId == null ) {
                    MapQuery.put(filekey, null);
                }

                Message msgtosend = new Message();
                msgtosend.msgType ="5";
                if(senderNodeId == null) {
                    msgtosend.senderNodeID=myNodeId;
                } else {
                    msgtosend.senderNodeID=senderNodeId;
                }

                msgtosend.successor=SenderHashMap.get(succnode);
                msgtosend.typeofquery=filekey;
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgtosend);
                Log.i("my", "Query(Direct):msg send to Client task" + msgtosend);

                if(senderNodeId==null) {
                    while(MapQuery.get(filekey)==null){
                    }
                    fileData.append(MapQuery.get(filekey));
                }
            }
            MatrixCursor mc = new MatrixCursor(new String[] { "key", "value"});
            mc.addRow(new String[] {filekey, fileData.toString() });
            Log.i("my","query(direct): end");
            return mc;
        }
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];

            while (true) {
                try {
                    Socket socket = serverSocket.accept();
                    ObjectInputStream inputstream = new ObjectInputStream(socket.getInputStream());
                    Message rcvdmessage = (Message) inputstream.readObject();
                    inputstream.close();
                    socket.close();

                    if (rcvdmessage.msgType.equalsIgnoreCase("1")) {
                        Log.i("my", "ServerTask(1):start");
                        String senderNodeID = rcvdmessage.senderNodeID;
                        String senderNodeIDGenHash= null;
                        try {
                            senderNodeIDGenHash = genHash(senderNodeID);
                        } catch (NoSuchAlgorithmException e) {
                            Log.e("my", "ServerTask(1):Gen hash exception:"+e.toString());
                            e.printStackTrace();
                        }
                        nodesList.add(senderNodeIDGenHash);

                        SortNodeList(nodesList);
                        Message msgtosend = new Message();
                        msgtosend.msgType="2";
                        msgtosend.nodesList=nodesList;
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgtosend);
                        Log.i("my", "ServerTask(1): msg send to Client task:"+msgtosend.toString());
                        Log.i("my", "ServerTask(1):end");

                    } else if (rcvdmessage.msgType.equalsIgnoreCase("2")) {
                        Log.i("my", "ServerTask(2):start");
                        nodesList = rcvdmessage.nodesList;
                        int myNodeIndex = nodesList.indexOf(myNodeIdHash);
                        int predecessorIndex = myNodeIndex-1;
                        int successorIndex = myNodeIndex + 1;
                        if(myNodeIndex == 0){
                            predecessorIndex = nodesList.size()-1;
                        }
                        if (successorIndex >nodesList.size()-1) {
                            successorIndex = 0;
                        }
                        prednode = nodesList.get(predecessorIndex);
                        succnode = nodesList.get(successorIndex);
                        Log.i("my", "ServerTask(2):end");
                    } else if (rcvdmessage.msgType.equalsIgnoreCase("3")) {
                        Log.i("my", "ServerTask(3):start");
                        ContentValues tempCV = new ContentValues();
                        tempCV.put("key",(String)rcvdmessage.ContentValues.get("key"));
                        tempCV.put("value",(String)rcvdmessage.ContentValues.get("value"));

                        insert(null,tempCV );
                        Log.i("my", "ServerTask(3):end");
                    } else if (rcvdmessage.msgType.equalsIgnoreCase("5")) {
                        Log.i("my", "ServerTask(5):start");
                        Cursor cursor = query(myUri, new String[]{rcvdmessage.senderNodeID}, rcvdmessage.typeofquery, null, null);
                        if (cursor== null || cursor.getCount()==0) {
                            Log.i("my", "ServerTask(5): query result is null");

                        } else {
                            int Indexkey = cursor.getColumnIndex("key");
                            int Indexvalue = cursor.getColumnIndex("value");

                            if (Indexkey == -1 || Indexvalue == -1) {
                                Log.i("my", "Servertask(5): invalid columns");
                                cursor.close();
                            }

                            cursor.moveToFirst();

                            if (!(cursor.isFirst() && cursor.isLast())) {
                                cursor.close();
                            }

                            String resultkey = cursor.getString(Indexkey);
                            String resultvalue= cursor.getString(Indexvalue);
                            if(resultvalue!=null && resultvalue!="") {
                                Message msgtosend = new Message();
                                msgtosend.msgType="6";
                                msgtosend.ContentValues.put("key",resultkey);
                                msgtosend.ContentValues.put("value",resultvalue);
                                msgtosend.successor=rcvdmessage.senderNodeID;
                                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgtosend);
                                Log.i("my", "Servertask(5):msg send to Client task:"+msgtosend.toString());
                            }
                            Log.i("my", "ServerTask(5):end");
                        }
                    } else if (rcvdmessage.msgType.equalsIgnoreCase("6")) {
                        Log.i("my", "ServerTask(6):start");
                        MapQuery.put((String)rcvdmessage.ContentValues.get("key"), (String)rcvdmessage.ContentValues.get("value"));
                        Log.i("my", "ServerTask(6):end");
                    }else if (rcvdmessage.msgType.equalsIgnoreCase("7")) {
                        Log.i("my", "ServerTask(7):start");
                        String rcvdsenderNodeId = rcvdmessage.senderNodeID;
                        String selectionParameter   = rcvdmessage.typeofquery;

                        Cursor cursor = query(myUri, new String[]{"7",rcvdsenderNodeId}, selectionParameter, null, null);
                        if (cursor== null || cursor.getCount()<=0) {
                            Log.i("my", "Servertask(7):query Result is null");
                            Message msgtosend = new Message();
                            msgtosend.msgType="8";
                            msgtosend.ContentValues=null;
                            msgtosend.successor=rcvdsenderNodeId;
                            msgtosend.senderofrepplyingall=myNodeId;
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgtosend);
                            Log.i("my", "Servertask(7): msg send to Client task with msg"+msgtosend.toString());
                            Log.i("my", "ServerTask(7):end");
                        } else {
                            int Indexkey = cursor.getColumnIndex("key");
                            int Indexvalue = cursor.getColumnIndex("value");

                            if (Indexkey == -1 || Indexvalue == -1) {
                                Log.i("my", "servertask(7): invalid columns");
                                cursor.close();
                            }

                            cursor.moveToFirst();

                            String returnFilekey = null;
                            String returnFileData = null;
                            int i = 0 ;
                            HashMap<String,String> Mapfile = new HashMap<String,String>();

                            returnFilekey= cursor.getString(Indexkey);
                            returnFileData= cursor.getString(Indexvalue);
                            if(returnFileData!=null && returnFileData!="") {
                                Mapfile.put(returnFilekey,returnFileData);
                            }

                            while(cursor.moveToNext()) {
                                i++;
                                returnFilekey= cursor.getString(Indexkey);
                                returnFileData= cursor.getString(Indexvalue);
                                if(returnFileData!=null && returnFileData!="") {
                                    Mapfile.put(returnFilekey,returnFileData);
                                }
                            }
                            Message msgtosend = new Message();
                            msgtosend.msgType="8";
                            msgtosend.ContentValues=Mapfile;
                            msgtosend.senderofrepplyingall=myNodeId;
                            msgtosend.successor=rcvdsenderNodeId;
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgtosend);
                        }
                        Log.i("my", "ServerTask(7):end");
                    } else if (rcvdmessage.msgType.equalsIgnoreCase("8")) {
                        Log.i("my", "ServerTask(8):start");
                        String rcvdsenderofrepplyingallMsg = rcvdmessage.senderofrepplyingall;
                        MapQueryAll.put(rcvdsenderofrepplyingallMsg,(HashMap)rcvdmessage.ContentValues);
                        CountQueryAll++;
                        Log.i("my", "ServerTask(8):end");
                    }else if (rcvdmessage.msgType.equalsIgnoreCase("4")) {
                        Log.i("my", "ServerTask(4):start");
                        delete(myUri, rcvdmessage.typeofquery,new String[]{"4", rcvdmessage.senderNodeID});
                        Log.i("my", "ServerTask(8):end");
                    } else if (rcvdmessage.msgType.equalsIgnoreCase("9")) {
                        Log.i("my", "ServerTask(9):start");
                        delete(myUri, "@",new String[]{"9", rcvdmessage.senderNodeID});
                        Log.i("my", "ServerTask(9):end");
                    }
                } catch (Exception e) {
                    Log.i("my","Servertask():exception:"+e.toString());
                    e.printStackTrace();
                }
            }
        }
        protected void onProgressUpdate(String... strings) {

        }
    }

    private class ClientTask extends AsyncTask<Message, Void, Void> {

        @Override
        protected Void doInBackground(Message... msgs) {
            Message msg = msgs[0];
            if (msg.msgType.equalsIgnoreCase("1"))  {
                Log.i("my", "Clienttask(1):start");
                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(Ports[0]));
                    ObjectOutputStream outputstream = new ObjectOutputStream((socket.getOutputStream()));
                    outputstream.writeObject(msg);
                    outputstream.flush();
                } catch (IOException e) {
                    Log.e("my", "ClientTask(1): socket IOException"+e.toString());

                } catch (Exception e) {
                    Log.e("my", "Client task(1): socket Exception"+e.toString());
                }
                Log.i("my", "Clienttask(1):end");
            }else if (msg.msgType.equalsIgnoreCase("2")) {
                Log.i("my","client task(2):start");
                for(String tempalivenodehash : nodesList) {
                    String tempport = SenderHashMap.get(tempalivenodehash);
                    try {
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(tempport) * 2);
                        ObjectOutputStream outputstream = new ObjectOutputStream(socket.getOutputStream());
                        outputstream.writeObject(msg);
                        outputstream.flush();
                    } catch (IOException e) {
                        Log.e("my", "ClientTask(2): socket IOException" + e.toString());

                    } catch (Exception e) {
                        Log.e("my", "Client task(2): socket Exception" + e.toString());
                    }
                    Log.i("my", "Clienttask(2):end");
                }
            } else if (msg.msgType.equalsIgnoreCase("3"))  {
                Log.i("my", "Clienttask(3):start");
                try {
                    int ReceiverPort = Integer.parseInt(msg.successor)*2;
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            ReceiverPort);
                    ObjectOutputStream outputstream= new ObjectOutputStream((socket.getOutputStream()));
                    outputstream.writeObject(msg);
                    outputstream.flush();


                } catch (IOException e) {
                    Log.e("my", "ClientTask(3): socket IOException"+e.toString());

                } catch (Exception e) {
                    Log.e("my", "Client task(3): socket Exception"+e.toString());
                }
                Log.i("my", "Clienttask(3):end");
            } else if (msg.msgType.equalsIgnoreCase("5"))  {
                Log.i("my", "Clienttask(5):start");
                try {
                    int ReceiverPort = Integer.parseInt(msg.successor)*2;
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            ReceiverPort);
                    ObjectOutputStream outputstream= new ObjectOutputStream((socket.getOutputStream()));
                    outputstream.writeObject(msg);
                    outputstream.flush();
                } catch (IOException e) {
                    Log.e("my", "ClientTask(5): socket IOException"+e.toString());

                } catch (Exception e) {
                    Log.e("my", "Client task(5): socket Exception"+e.toString());
                }
                Log.i("my", "Clienttask(5):end");
            } else if (msg.msgType.equalsIgnoreCase("6"))  {
                Log.i("my", "Clienttask(6):start");
                try {
                    int ReceiverPort = Integer.parseInt(msg.successor)*2;
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            ReceiverPort);
                    ObjectOutputStream outputstream= new ObjectOutputStream((socket.getOutputStream()));
                    outputstream.writeObject(msg);
                    outputstream.flush();


                } catch (IOException e) {
                    Log.e("my", "ClientTask(6): socket IOException"+e.toString());

                } catch (Exception e) {
                    Log.e("my", "Client task(6): socket Exception"+e.toString());
                }
                Log.i("my", "Clienttask(6):end");
            }else if (msg.msgType.equalsIgnoreCase("7")) {
                Log.i("my", "Clienttask(7):start");
                try {
                    int ReceiverPort = Integer.parseInt(msg.successor) * 2;
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            ReceiverPort);
                    ObjectOutputStream outputstream = new ObjectOutputStream((socket.getOutputStream()));
                    outputstream.writeObject(msg);
                    outputstream.flush();


                } catch (IOException e) {
                    Log.e("my", "ClientTask(7): socket IOException"+e.toString());

                } catch (Exception e) {
                    Log.e("my", "Client task(7): socket Exception"+e.toString());
                }
                Log.i("my", "Clienttask(7):end");

            } else if (msg.msgType.equalsIgnoreCase("8"))  {
                Log.i("my", "Clienttask(8):start");
                try {
                    int ReceiverPort = Integer.parseInt(msg.successor)*2;
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            ReceiverPort);
                    ObjectOutputStream outputstream= new ObjectOutputStream((socket.getOutputStream()));
                    outputstream.writeObject(msg);
                    outputstream.flush();
                } catch (IOException e) {
                    Log.e("my", "ClientTask(8): socket IOException"+e.toString());

                } catch (Exception e) {
                    Log.e("my", "Client task(8): socket Exception"+e.toString());
                }
                Log.i("my", "Clienttask(8):end");
            } else if (msg.msgType.equalsIgnoreCase("4") || msg.msgType.equalsIgnoreCase("9"))  {
                Log.i("my", "Clienttask(4):start");
                try {
                    int ReceiverPort = Integer.parseInt(msg.successor)*2;
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            ReceiverPort);
                    ObjectOutputStream outputstream = new ObjectOutputStream((socket.getOutputStream()));
                    outputstream.writeObject(msg);
                    outputstream.flush();


                } catch (IOException e) {
                    Log.e("my", "ClientTask(4): socket IOException"+e.toString());

                } catch (Exception e) {
                    Log.e("my", "Client task(4): socket Exception"+e.toString());
                }
                Log.i("my", "Clienttask(4):end");
            }
            return null;
        }
    }

    public void SortNodeList(ArrayList<String> list)
    {
        Log.i("my","SortNodeList start");
        Collections.sort(list, new Comparator<String>() {
            @Override
            public int compare(String s1, String s2) {
                return s1.compareToIgnoreCase(s2);
            }
        });
        Log.i("my","SortNodeList end");
    }
    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
}
