import java.io.Serializable;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.Scanner;

public class Replica extends UnicastRemoteObject implements IRMI_Replica, Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	Database database ;
	String replicaId;
	protected Replica(String replicaId, boolean recovery) throws RemoteException {
		super();
		System.out.println("Inside Replica Constructor");
		this.replicaId = replicaId;
		database = new Database(replicaId);
		if(recovery) {
			System.out.println("Calling Recovery");
			database.recoverDB();
		}
		// TODO Auto-generated constructor stub
	}
	
	
	@Override
	public String canCommit(String requestType, int key, String value, String transactionId) throws RemoteException {
		// TODO Auto-generated method stub
		System.out.println("Inside do commit");
		try {
			if(requestType.equals("Put")) {
					database.put(key, value, this.replicaId, transactionId);
					System.out.println("Committed Request for Put ");
					return "VOTE_COMMIT";
					
			} else if(requestType.equals("Delete")) {
				database.delete(key, this.replicaId, transactionId);
				System.out.println("Committed Request for Delete ");
				return "VOTE_COMMIT";
			}
		}catch (Exception e) {
			System.out.println(e.getMessage());
			
		}
				
		return "Problem while commiting";
	}

	@Override
	public String doCommit(String requestType, int key, String value, String transactionId) throws RemoteException {
		// TODO Auto-generated method stub
		database.commit(requestType,  key,  value, this.replicaId, transactionId);
		return "COMMITED";
	}

	@Override
	public String doAbort(String requestType, int key, String value) throws RemoteException {
		database.abort(requestType, key, value, this.replicaId);
		return "Aborted";
	}


	@Override
	public String get(int key) throws RemoteException {
		return database.get(key);
	}

/*
	@Override
	public String getAll() throws RemoteException {
		System.out.println("Inside Replica**********");
	//	Database db = new Database();
		//db.getAll();
		database.getAll();
		return "hereeee";
	}

	public static void main(String[] args) {
		System.out.println("---");
		Database db = new Database();
		db.getAll();
	}*/
}
	
