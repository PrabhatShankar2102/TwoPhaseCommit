import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.AlreadyBoundException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Scanner;

public class Replica_Controller {

public static void main(String[] args) throws RemoteException, AlreadyBoundException, UnknownHostException, NotBoundException {
		
		try {
			Scanner scan = new Scanner(System.in);
			System.out.println("Please enter Unique Integer Id of the replica or same as previous if restarting after a failure");
			String replicaId = "_Replica"+scan.nextInt();
			System.out.println("If Starting after a failure, do you need recovery to run?");
			boolean recover = false ;
			System.out.println("Yes \n No");
			String choice = scan.next();
			System.out.println(choice);
			switch (choice) {
			case "Yes":
				 recover = true;
				break;
			case "No":
				recover = false ;
				break;
			
			default:
				break;
			}
			
			String ipAddress = InetAddress.getLocalHost().getHostAddress();
			System.out.println("IP of Replica: " +ipAddress );
			int portNumber = 1200;
			Registry reg = LocateRegistry.createRegistry(portNumber);
			
			IRMI_Replica replica = new Replica(replicaId, recover);
			reg.bind("Replica", replica);
			System.out.println("Replica running on port 1200");
			
			IRMI_Master masterServer = null ;
			String serverIp = null;
			String incomigMessage = "";
			int serverPort= 0;
		
			System.out.println("Enter Master IP and Master PortNumber");
			serverIp = scan.next();
			serverPort = scan.nextInt();
			
			Registry myReg;
			myReg = LocateRegistry.getRegistry(serverIp,serverPort);
			masterServer = (IRMI_Master) myReg.lookup("Bootstrap");
			incomigMessage= masterServer.connect(ipAddress, portNumber);
			System.out.println("Replica is Running and is connected to Master \n"+ incomigMessage);
		    
			}catch (UnknownHostException e) {
				e.printStackTrace();
			}catch (Exception e) {
				System.out.println("-----"+e.getMessage());
			}
		}
}
