package phase2Pack;
 
import java.io.*;
import java.net.*;
import java.util.*;
//import com.lixingyu.eece411.A1.*;

import com.matei.eece411.util.*;

public class Client {
    private static Socket socket;
    private static final int BUFSIZE = 1057;   
    private static final int INTSIZE = 1057; //bytes
    
    public static void main(String[] args)
    {
        try {
        	if (args.length != 3)
    		{
    			System.out.println("USAGE: Server " + "<hostname/IP> <port> <command>");
    			System.exit(0);
    		}
        	
        	String server = (args.length == 3) ? args[0] : "Rocky-PC"; // Server name or IP address
            int servPort = (args.length == 3) ? Integer.parseInt(args[1]) : 7;
		    int cmd = (args.length == 3) ? Integer.parseInt(args[2]) : 1; //1: put, 2: get, 3: remove 
		    //int key = Integer.parseInt(args[3]);
		    //int value = Integer.parseInt(args[4]);
	
		    byte[] byteBuffer = new byte[INTSIZE];
		    byte[] recvBuffer = new byte[BUFSIZE];
	
		    ByteOrder.int2leb(cmd, byteBuffer, 0); 	//Command byte - 1 byte
		    ByteOrder.int2leb(1, byteBuffer, 1); 	//Key bytes - 32 bytes
		    ByteOrder.int2leb(1, byteBuffer, 33); 	//Value bytes - 1024 bytes
		    
		    // Create socket that is connected to server on specified port
            //byteBuffer[0] = (byte)1;
		    System.out.println(Arrays.toString(byteBuffer));
		    socket = new Socket(server, servPort);
            System.out.println("Connected to server...");
	 	
		    //Send the message to the server
            OutputStream os = socket.getOutputStream();
		    os.write(byteBuffer);  // Send the encoded string to the server	
 
            //Get the return message from the server
            InputStream is = socket.getInputStream();
	
		    int totalBytesRcvd = 0;  // Total bytes received so far
            int bytesRcvd;           // Bytes received in last read

            while (totalBytesRcvd < byteBuffer.length)
            {
            	if ((bytesRcvd = is.read(recvBuffer, totalBytesRcvd,  
            			recvBuffer.length - totalBytesRcvd)) != -1)
				{
					totalBytesRcvd += bytesRcvd;
				}
		        StdLog(recvBuffer);
		    }
	
        } catch (Exception exception) {
        	exception.printStackTrace();
        } finally {
            //Closing the socket
            try {
                socket.close();
            } catch(Exception e) {
                e.printStackTrace();
            }
        }
    }

    private static void StdLog(byte[] msg)
    {	   	
		//declare subset byte[]
		byte[] msgLenByte = new byte[INTSIZE];	
		byte[] codeLenByte = new byte[INTSIZE];
		byte[] secret = new byte[msg.length-12];
		
		//split byte stream
		msgLenByte = Arrays.copyOfRange(msg,0,4);
		codeLenByte = Arrays.copyOfRange(msg, 8,12);
		secret = Arrays.copyOfRange(msg, 12, msg.length);
		
		//little endian to int
		int msgLength = ByteOrder.leb2int(msgLenByte,0);
		int codeLength = ByteOrder.leb2int(codeLenByte,0);
	
		//print out
		if (codeLength != 0)
		{
			//sanity check
       		System.out.println("Byte Message Received: " + StringUtils.byteArrayToHexString(msg));
			
			//print result to console
			System.out.println("Message Length: " + msgLength);
			System.out.println("Code Length: " + codeLength);
			System.out.println("Secret Received: " + StringUtils.byteArrayToHexString(secret));	
    	} 
    }
}

