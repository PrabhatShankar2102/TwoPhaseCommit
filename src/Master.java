import java.io.IOException;
import java.io.Serializable;
import java.net.ConnectException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;

public class Master extends UnicastRemoteObject  implements IRMI_Master, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	 IRMI_Replica replica = null ;
	 Registry myReg;
	 final long timeout = 5000;
	 boolean stateChanging = false ;
	 Database database;
	 boolean proceed = false ;	
	 static int count = 0;
	//Hold the address of all the replicas
	Map<String, Integer> replicasMap ;
	List<Integer> stateChangeKeys;
	ReentrantLock lock ;
	protected Master(boolean recovery) throws IOException {
		super();
		System.out.println("Master Construcor");
		database = new Database("_Master");
		
		replicasMap = new HashMap(); 
		stateChangeKeys = new ArrayList();
		lock = new ReentrantLock();
		if(recovery) {
			database.recoverDB();
		}
	}
	
	//Check if state change operation is allowed
	public boolean isSafe(int key) {
		boolean safe = false ;
		try {
			System.out.println("Inside is safe");
			lock.lock();
		
			if(!stateChangeKeys.contains(key)) {
				System.out.println("Inside If Safe");
				stateChangeKeys.add(key);
				safe = true ;
			}
		}catch (Exception e) {
			System.out.println("Exception in isSafe "+e.getMessage());
		} finally {
			lock.unlock();
		}
		
		return safe; 
	}
	
	
	//Remove the key from List once done
	public void releaseKey(int key) {
		System.out.println("Before: " +stateChangeKeys.size());
		try {
			System.out.println("Inside rel");
			lock.lock();
			stateChangeKeys.remove(Integer.valueOf(key));
		} catch (Exception e) {
			System.out.println("Exception in releaseKey "+e.getMessage());
		} finally {
			lock.unlock();
		}
		System.out.println("Aftr: " +stateChangeKeys.size());
	}

	@Override
	public String put(int key, String value) throws RemoteException {
		// TODO Auto-generated method stub
		String message = "" ;
		boolean safe = isSafe(key);
		System.out.println("Inside Put");
		//Each transaction will have UUID, For recovery purposes
		String transactionId = UUID.randomUUID().toString();
		System.out.println("Transaction Id  for this put is "+transactionId);
		if(safe) {
				String requestType = "Put";
				Map<String, Integer> replicasDoCommitMap =  new HashMap();
				Map<String, Integer> replicasNoResponseCommitMap =  new HashMap();
				List <String> responses = new ArrayList();
				database.put(key, value, "_Master",  transactionId);
				try {
						//BoraodCast Can Commit
						for (Entry<String,Integer> pair : replicasMap.entrySet()){	
				            System.out.println(pair.getKey()+" "+pair.getValue());
				    		String replicaIp = pair.getKey();
				    		int replicaPort = pair.getValue();
				    		try {
								myReg = LocateRegistry.getRegistry(replicaIp,replicaPort);
								replica = (IRMI_Replica) myReg.lookup("Replica");
								String response = replica.canCommit(requestType, key, value, transactionId);
								responses.add(response);
								if(response.equals("VOTE_COMMIT")) {
									replicasDoCommitMap.put(replicaIp, replicaPort);
								}
				    		}catch (Exception e) {
				    			// Add to unsuccessful Map
				    			replicasNoResponseCommitMap.put(replicaIp, replicaPort);
				    		}
						}
					
						/*
						 * Retry Logic
						 * If replicasDoCommitMap is less than replicasMap then it means one of the replica is down wait 15 secs 
						 * for recovery and try again
						 */
						if(replicasDoCommitMap.size()<replicasMap.size()) {
							System.out.println("One or more Replica is down will Retry in 15 seconds");
							Thread.sleep(15000);
							System.out.println("Trying to contact down replicas");
							//Retry only for Not responding Replica's
							for (Entry<String,Integer> pair : replicasNoResponseCommitMap.entrySet()){
					            System.out.println(pair.getKey()+" "+pair.getValue());
					    		String replicaIp = pair.getKey();
					    		int replicaPort = pair.getValue();
					    		System.out.println("Trying to contact replica at "+replicaIp);
					    		try {
									myReg = LocateRegistry.getRegistry(replicaIp,replicaPort);
									replica = (IRMI_Replica) myReg.lookup("Replica");
									String response = replica.canCommit(requestType, key, value,  transactionId);
									responses.add(response);
									if(response.equals("VOTE_COMMIT")) {
										replicasDoCommitMap.put(replicaIp, replicaPort);
									}
					    		}catch (Exception e) {
									System.out.println("Replica at "+replicaIp+ " is still down.");
									break ;
								} 
							}
						}
						
						
						
						/*
						 * Case1 Someone voted Abort  or is Still down
						 * ABORT!!!!!!
						 */
						if(replicasDoCommitMap.size()<replicasMap.size()) {
							System.out.println("We need to Abort....");
							System.out.println("Sending Abort to all the respondents ");
							//Reply only to responded Replicas 
							database.abort(requestType, key, value, "_Master");
							for (Entry<String,Integer> pair : replicasDoCommitMap.entrySet()){
					            System.out.println(pair.getKey()+" "+pair.getValue());
					    		String replicaIp = pair.getKey();
					    		int replicaPort = pair.getValue();
								myReg = LocateRegistry.getRegistry(replicaIp,replicaPort);
								replica = (IRMI_Replica) myReg.lookup("Replica");
								//Send Abort
								replica.doAbort(requestType, key, value) ;
								message = "Put Operation Aborted";
					        }
							releaseKey(key);
						}
						
						/*
						 * Case 2 everyone agrees 
						 * DO COMITTT!!
						 */
						else if(replicasDoCommitMap.size()==replicasMap.size()) {
						//Proceed with the Transaction
							database.commit(requestType, key, value, "_Master",  transactionId);
							for (Entry<String,Integer> pair : replicasMap.entrySet()){
				            System.out.println(pair.getKey()+" "+pair.getValue());
				    		String replicaIp = pair.getKey();
				    		int replicaPort = pair.getValue();
							myReg = LocateRegistry.getRegistry(replicaIp,replicaPort);
							replica = (IRMI_Replica) myReg.lookup("Replica");
							replica.doCommit(requestType, key, value, transactionId);
							message = "Put Operation Suceesful";
							}
							releaseKey(key);
						}
					
				} catch (Exception e) {
					System.out.println("Exception in PUT: "+e.getMessage());
				}
		} else {
			System.out.println("Resource : "+key+" is locked");
		}
		return message ;
	}

	@Override
	public String get(int key) throws RemoteException {
		System.out.println("Inside get");
		
		List <String> list = new ArrayList(replicasMap.keySet());
		System.out.println("-----"+list.size());
		if(list.size()>0) {
		Random rand = new Random();
		//Get Random Replica Ip
		String replicaIp = list.get(rand.nextInt(replicasMap.size()));
		int replicaPort = replicasMap.get(replicaIp);
		try {
	    		//Registry myReg;
				myReg = LocateRegistry.getRegistry(replicaIp,replicaPort);
				replica = (IRMI_Replica) myReg.lookup("Replica");
				
				System.out.println("Hereee");
				//System.out.println(replica.getAll());
				count = 0;
				
		 }  catch (Exception e ) {
			 count++;
			 if(count < 10){
				 System.out.println("Connected Down replica, trying again "+count+"th time");
				 get(key);
			 } else {
				System.out.println("Max retry used");
				System.out.println(e.getMessage());
				return database.get(key);
			 }
			
		}
		}else {
			System.out.println("No Replicas, Returning from Master");
			return database.get(key);
		}
		
	 return replica.get(key);
	}

	@Override
	public String delete(int key) throws RemoteException {
		System.out.println("Inside Del");
		String message = "" ;
		String transactionId = UUID.randomUUID().toString();
		System.out.println("Transaction Id  for this Delete is "+transactionId);
		boolean safe = isSafe(key);
		if(safe) {
				String requestType= "Delete" ;
				String value = "";
				List <String> responses = new ArrayList();
				Map<String, Integer> replicasDoCommitMap =  new HashMap();
				Map<String, Integer> replicasNoResponseCommitMap =  new HashMap();
				database.delete(key, "_Master", transactionId);
				try {
						//BoraodCast Can Commit
						for (Entry<String,Integer> pair : replicasMap.entrySet()){	
				            System.out.println(pair.getKey()+" "+pair.getValue());
				    		String replicaIp = pair.getKey();
				    		int replicaPort = pair.getValue();
				    		try {
								myReg = LocateRegistry.getRegistry(replicaIp,replicaPort);
								replica = (IRMI_Replica) myReg.lookup("Replica");
								String response = replica.canCommit(requestType, key, value, transactionId);
								responses.add(response);
								if(response.equals("VOTE_COMMIT")) {
									replicasDoCommitMap.put(replicaIp, replicaPort);
								}
				    		}catch (Exception e) {
				    			// Add to unsuccessful Map
				    			replicasNoResponseCommitMap.put(replicaIp, replicaPort);
				    		}
						}
						
						
						
						/*
						 * Retry Logic
						 * If replicasDoCommitMap is less than replicasMap then it means one of the replica is down wait 15 secs 
						 * for recovery and try again
						 */
						if(replicasDoCommitMap.size()<replicasMap.size()) {
							System.out.println("One or more Replica is down will Retry in 15 seconds");
							Thread.sleep(15000);
							System.out.println("Trying to contact down replicas");
							//Retry only for Not responding Replica's
							for (Entry<String,Integer> pair : replicasNoResponseCommitMap.entrySet()){
					            System.out.println(pair.getKey()+" "+pair.getValue());
					    		String replicaIp = pair.getKey();
					    		int replicaPort = pair.getValue();
					    		System.out.println("Trying to contact replica at "+replicaIp);
					    		try {
									myReg = LocateRegistry.getRegistry(replicaIp,replicaPort);
									replica = (IRMI_Replica) myReg.lookup("Replica");
									String response = replica.canCommit(requestType, key, value,  transactionId);
									responses.add(response);
									if(response.equals("VOTE_COMMIT")) {
										replicasDoCommitMap.put(replicaIp, replicaPort);
									}
					    		}catch (Exception e) {
									System.out.println("Replica at "+replicaIp+ " is still down.");
									break ;
								} 
							}
						}
						
						
						/*
						 * Case1 Someone voted Abort  or is Still down
						 * ABORT!!!!!!
						 */
						if(replicasDoCommitMap.size()<replicasMap.size()) {
							System.out.println("We need to Abort....");
							System.out.println("Sending Abort to all the respondents ");
							database.abort(requestType, key, value, "_Master");
							//Reply only to responded Replicas 
							for (Entry<String,Integer> pair : replicasDoCommitMap.entrySet()){
					            System.out.println(pair.getKey()+" "+pair.getValue());
					    		String replicaIp = pair.getKey();
					    		int replicaPort = pair.getValue();
								myReg = LocateRegistry.getRegistry(replicaIp,replicaPort);
								replica = (IRMI_Replica) myReg.lookup("Replica");
								//Send Abort
								replica.doAbort(requestType, key, value) ;
								message = "Delete Operation Aborted";
					        }
							releaseKey(key);
						}
						
						/*
						 * Case 2 everyone agrees 
						 * DO COMITTT!!
						 */
						else if(replicasDoCommitMap.size()==replicasMap.size()) {
						//Proceed with the Transaction
							database.commit(requestType, key, value, "_Master", transactionId);
							for (Entry<String,Integer> pair : replicasMap.entrySet()){
				            System.out.println(pair.getKey()+" "+pair.getValue());
				    		String replicaIp = pair.getKey();
				    		int replicaPort = pair.getValue();
							myReg = LocateRegistry.getRegistry(replicaIp,replicaPort);
							replica = (IRMI_Replica) myReg.lookup("Replica");
							replica.doCommit(requestType, key, value, transactionId);
							message = "Delete Operation Suceesful";
							}
							releaseKey(key);
						}
					
				} catch (Exception e) {
					System.out.println("Exception in Delete: "+e.getMessage());
				}
		}	
		else {
			System.out.println("Resource : "+key+" is locked");
		}
		return message ;
	}

	@Override
	public String connect(String ipAddress, int portNumber) throws RemoteException  {
		replicasMap.put(ipAddress, portNumber);
		
		for (Entry<String,Integer> pair : replicasMap.entrySet()){
            System.out.println(pair.getKey()+" "+pair.getValue());
        }
		return  "There are "+ replicasMap.size()+" now";
	}
	
}
