import java.rmi.Remote;
import java.rmi.RemoteException;

public interface IRMI_Replica extends Remote{

	public String canCommit(String requestType, int key, String value, String transactionId) throws RemoteException;
	public String doCommit(String requestType, int key, String value, String transactionId) throws RemoteException;
	public String doAbort(String requestType, int key, String value) throws RemoteException;
	public String get(int key) throws RemoteException;
	//public String getAll() throws RemoteException;
}
