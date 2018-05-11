import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.RMIClientSocketFactory;
import java.rmi.server.RMIServerSocketFactory;
import java.rmi.server.UnicastRemoteObject;
import java.util.Scanner;

public class RMI_Client {
	
	 static Registry myReg;
	 static IRMI_Master masterServer = null ;
	 static void create(String serverIp, int serverPort) {
		 try {
			myReg = LocateRegistry.getRegistry(serverIp,serverPort);
			masterServer = (IRMI_Master) myReg.lookup("Bootstrap");
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NotBoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
			
	 }
	public static void main(String[] args) {
		
		String incomigMessage = "";
		String serverIp = null;
		int serverPort= 0;
		Scanner scan = new Scanner(System.in);
		System.out.println("Enter Server IP and Server PortNumber");
		serverIp = scan.next();
		serverPort = scan.nextInt();
		
		
		try {
			create(serverIp, serverPort);
			
			while(true) {
				System.out.println("\nPlease select an option");
				System.out.println("Put");
				System.out.println("Delete");
				System.out.println("Get");
				String requestType = scan.next();
		        int key ;
				switch(requestType) {
				case "Put" :
					System.out.println("Please enter integer key ");
					key = scan.nextInt();
					System.out.println("Please enter value ");
					String value = scan.next();
					try {
					incomigMessage = masterServer.put(key, value);
					}catch (java.rmi.ConnectException e) {
						System.out.println("Unable to connect to Serevr, Will try again in 10 seconds");
						try {
							Thread.sleep(10000);
							create(serverIp, serverPort);
							incomigMessage = masterServer.put(key, value);
							
						} catch (InterruptedException e1) {
							// TODO Auto-generated catch block
							e1.printStackTrace();
						}
					} 
					System.out.println(incomigMessage);
					break;
				case "Delete" :
					System.out.println("Please enter a integer key to delete it and it's associated value ");
					 key = scan.nextInt();
					 try {
					 incomigMessage = masterServer.delete(key);
					 }catch (java.rmi.ConnectException e) {
							System.out.println("Unable to connect to Serevr, Will try again in 10 seconds");
							try {
								Thread.sleep(10000);
								create(serverIp, serverPort);
								incomigMessage = masterServer.delete(key);
								
							} catch (InterruptedException e1) {
								// TODO Auto-generated catch block
								e1.printStackTrace();
							}
						} 
					 System.out.println(incomigMessage);
					break;
				case "Get":
					System.out.println("Please enter a integer key to get it's value");
					key  = scan.nextInt();
					try {
					incomigMessage = masterServer.get(key);
					}catch (java.rmi.ConnectException e) {
						System.out.println("Unable to connect to Serevr, Will try again in 10 seconds");
						try {
							Thread.sleep(10000);
							create(serverIp, serverPort);
							incomigMessage = masterServer.get(key);
						} catch (InterruptedException e1) {
							// TODO Auto-generated catch block
							e1.printStackTrace();
						}
					} 
					System.out.println(incomigMessage);
					break;
				default:
					System.out.println("Please enter a valid option");
					break;
				}
			
		} 
			
		}
		catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		
	}

}
