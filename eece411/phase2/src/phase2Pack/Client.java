package phase2Pack;

import java.io.*;
import java.net.*;
import java.util.*;

public class Client {
	private static final int CMD_SIZE = 1;
	private static final int KEY_SIZE = 32;
	private static final int VALUE_SIZE = 1024;
	private static final int ERR_SIZE = 1;
	private static final int MAX_NUM_CLIENTS = 50;

	private static String server;
	private static int servPort;
	private static int cmd;
	private static int key;
	private static int value;

	public static void main(String[] args)
	{
		if (args.length < 4 || args.length > 5)
		{
			System.out.println("USAGE: Client " + "<hostname/IP> <port> <command> <key> <(optional) value>");
			System.out.println("Commands: Put = 1, Get = 2, Remove = 3");
			System.exit(0);
		}

		server = args[0]; //Server name or IP address
		servPort = Integer.parseInt(args[1]); //Server port
		cmd = Integer.parseInt(args[2]); //Command
		key = Integer.parseInt(args[3]); //Key

		value = 0; //Value for put command
		if (cmd == 1)
		{
			//If command is put, make sure a value parameter is given
			if (args.length != 5)
			{
				System.out.println("Please specify a value to map the key to for the put command!");
				return;
			}
			value = Integer.parseInt(args[4]);
		}
	}

	private static class ClientThread implements Runnable
	{
		public void run()
		{
			Socket socket = null;
			try {
				//Create socket that is connected to server on specified port
				socket = new Socket(server, servPort);
				System.out.println("Connected to server: " + socket.getInetAddress().toString());

				//Send the message to the server
				OutputStream os = socket.getOutputStream();
				//If command is put, then increase request buffer size to include value bytes
				byte[] requestBuffer = (cmd == 1) ? new byte[CMD_SIZE + KEY_SIZE + VALUE_SIZE] : new byte[CMD_SIZE + KEY_SIZE];
				ByteOrder.int2leb(cmd, requestBuffer, 0); 			//Command byte - 1 byte
				ByteOrder.int2leb(key, requestBuffer, CMD_SIZE); 	//Key bytes - 32 bytes
				if (cmd == 1)
				{
					ByteOrder.int2leb(value, requestBuffer, 33); 	//Value bytes - 1024 bytes
				}

				//Send the encoded string to the server
				os.write(requestBuffer);
				System.out.println("Sending request:");
				System.out.println(StringUtils.byteArrayToHexString(requestBuffer));

				//Get the return message from the server
				InputStream is = socket.getInputStream();
				int totalBytesRcvd = 0;  // Total bytes received so far
				int bytesRcvd;           // Bytes received in last read
				byte[] replyBuffer = new byte[ERR_SIZE];
				while (totalBytesRcvd < replyBuffer.length)
				{
					if ((bytesRcvd = is.read(replyBuffer, totalBytesRcvd,
							replyBuffer.length - totalBytesRcvd)) != -1)
					{
						totalBytesRcvd += bytesRcvd;
					}
				}

				byte errCode = replyBuffer[0];
				System.out.println("Reply received:" + StringUtils.byteArrayToHexString(replyBuffer));
				System.out.println("Error Code: " + KVStore.errorMessage(errCode));

				//If command was get and ran successfully, then get the value bytes
				boolean cmdSuccessful = errCode == 0x00;
				byte[] getValue = new byte[VALUE_SIZE];
				if (cmd == 2 && cmdSuccessful)
				{
					int totalValueBytesRcvd = 0;  // Total bytes received so far
					int valueBytesRcvd;
					while (totalValueBytesRcvd < getValue.length)
					{
						if ((valueBytesRcvd = is.read(getValue, totalValueBytesRcvd,
								getValue.length - totalValueBytesRcvd)) != -1)
						{
							totalValueBytesRcvd += valueBytesRcvd;
						}
					}
					System.out.println("Value: " + StringUtils.byteArrayToHexString(getValue));
				}
			} catch (Exception exception) {
				exception.printStackTrace();
			} finally {
				//Closing the socket
				try {
					if (socket != null)
					{
						socket.close();
					}
				} catch(Exception e) {
					e.printStackTrace();
				}
			}
		}
	}
}

