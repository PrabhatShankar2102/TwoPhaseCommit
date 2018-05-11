import java.rmi.Remote;
import java.rmi.RemoteException;

public interface IRMI_Master extends Remote{

	public String put(int key, String value) throws RemoteException;
	public String get(int key) throws RemoteException;
	public String delete(int key) throws RemoteException;
	//For replicas to be connected
	public String connect(String ipAddress, int portNumber) throws RemoteException;
	
	
	
}
