import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.AlreadyBoundException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Scanner;

public class Master_Controller {

	
public static void main(String[] args) throws RemoteException, AlreadyBoundException, UnknownHostException, NotBoundException {
		
		try {
			Scanner scan = new Scanner(System.in);
			System.out.println("If Starting after a failure, do you need recovery to run?");
			boolean recover = false ;
			System.out.println("Yes \nNo");
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
			
			Registry reg = LocateRegistry.createRegistry(2102);
			IRMI_Master boot = new Master(recover);
			System.out.println("IP of Bootsrap: " + InetAddress.getLocalHost().getHostAddress());
			reg.bind("Bootstrap", boot);
			System.out.println("Bootstrap Server running on port 2102");
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}catch(Exception ex){
			ex.printStackTrace();
		}
	
	}
}
