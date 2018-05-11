import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class Database {
	Connection connection = null;
	Statement statement = null;
	String replicaId;
	Database(String replicaId){
		System.out.println("Inside Database Constructor");
		 this.replicaId = replicaId;
		 try {
			 Class.forName("org.sqlite.JDBC");
			connection = DriverManager.getConnection("jdbc:sqlite:assignment4"+replicaId+".db");
			System.out.println("DB Connection Opend successfully-- Constructor");
			
			statement = connection.createStatement();
			//Main Table to hold data
	         String sql = "Create Table IF NOT EXISTS Balances " +
	                        "(accountNumber INT PRIMARY KEY     NOT NULL," +
	                        "amount  TEXT NOT NULL)"; 
	         System.out.println("Query is  : "+sql);
	         statement.executeUpdate(sql);
	         System.out.println("Main Table created Successfully");
	         
	         //Backup Table to used for Rollback
	        /* String backUpSql = "Create Table IF NOT EXISTS LoggingTable"+
	        		 			"(accountNumber INT PRIMARY KEY     NOT NULL,"+
	        		 			"amount  VARCHAR NOT NULL)" +
	        		 			"oldValue VARCHAR "+
	        		 			"state TEXT" +
	        		 			"method TEXT)";
	         System.out.println("Backup Table created Successfully");
	         statement.executeUpdate(backUpSql);*/
	        
		} catch (SQLException e) {
			System.out.println("SQLException:   "+e.getMessage());
			e.printStackTrace();
		}catch (Exception e) {
			System.out.println("Exception:   "+e.getMessage());
		} finally {
			 try {
				statement.close();
				connection.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				System.out.println(e.getMessage());
			}
			 
		}
         
	}
	
	
	//Insert to DB
	public String put(int key, String value, String replicaId, String transactionId) {
		System.out.println("Inide DB, Request is for Put");
		String sql = null ;
		String message = null ;
		String oldValue = "null";
		 try {
			 	Class.forName("org.sqlite.JDBC");
			 	connection = DriverManager.getConnection("jdbc:sqlite:assignment4"+replicaId+".db");
				this.statement = connection.createStatement();
				System.out.println("DB Connection Opend successfully-- Put");
				//Check if record already exists, get count and update
				/*sql = "select count(*) as total from Balances where accountNumber = "+key ;
				sql = "select * from Balances where accountNumber = "+key ;  
				System.out.println("Query is  : "+sql);
				ResultSet rs= statement.executeQuery(sql)	*/;	
				sql = "select count(*) as total from Balances where accountNumber = "+key ;
				System.out.println("Query is  : "+sql);
				ResultSet rs= statement.executeQuery(sql)	;	
				int count = rs.getInt("total");
				System.out.println(count);
				
				if(count==0) {
					System.out.println("New");
					System.out.println("Insert Request");
					//Insert into Main Table
					sql = "INSERT INTO Balances (accountNumber, amount) " +  "VALUES ("+key+","+value+" );"; 
					System.out.println("Query is  : "+sql);
				      statement.executeUpdate(sql);
				      System.out.println("Key and Value inserted in DB Successfully");
				      //Update the backup Table incase of rollback
				     // UpdateBackupTable(key, value, oldValue, "VOTE_COMMIT", "Put");
				      writeArrivedLog(key, value, oldValue, "VOTE_COMMIT", "Insert", replicaId, transactionId);
				      
				      message = "Insert Successful ";
					
				}
				
				else  {
					//Update Request
					System.out.println("Update Request");
					ResultSet rs1 = statement.executeQuery("Select * from Balances where  accountNumber="+key);
					System.out.println(rs1.getFetchSize());
					while (rs1.next()) {
						 oldValue = rs.getString("amount");
						System.out.println("Update Request , Balance for "+key+" is "+oldValue);
					}
					System.out.println("Update Request, Od Value is "+oldValue );
					sql = "UPDATE Balances SET amount = " +value+ " where accountNumber = "+key ;
					System.out.println("Query is  : "+sql);
					statement.executeUpdate(sql);
					//Update the backup Table incase of rollback
				  //   UpdateBackupTable(key, value, oldValue, "VOTE_COMMIT", "Put");
					writeArrivedLog(key, value, oldValue, "VOTE_COMMIT", "Update", replicaId, transactionId);
					System.out.println("Update Succeesful");
					message = "Update Successful";
				}
					
		 	} catch (SQLException e) {
				System.out.println("SQLException during put operation :   "+e.getMessage());
			} catch (ClassNotFoundException e) {
				System.out.println(e.getMessage());
			} finally {
						try {
							statement.close();
							connection.close();
						} catch (SQLException e) {
							System.out.println(e.getMessage());
						}
			}
		 
		 return message ;
	}
	
	//Get the value of the key
		public String get(int key) {
			System.out.println("Inide DB, Request is for Get");
			String message = null;
			try{
				Class.forName("org.sqlite.JDBC");
				connection = DriverManager.getConnection("jdbc:sqlite:assignment4"+this.replicaId+".db");
				System.out.println("DB Connection Opend successfully-- Get");
				this.statement = connection.createStatement();
				ResultSet rs = statement.executeQuery("Select * from Balances where  accountNumber="+key);
				System.out.println(rs.getFetchSize());
				while (rs.next()) {
					String amount = rs.getString("amount");
					System.out.println("Balance for "+key+" is "+amount);
					message= "Balance for "+key+" is "+amount;
				}
				
				//connection.close();
			}catch (ClassNotFoundException e) {
				System.out.println(e.getMessage());
			} 
			catch (Exception e) {
				System.out.println("Exception in get key:   "+e.getMessage());
			}finally {
						try {
							statement.close();
							connection.close();
						} catch (SQLException e) {
							System.out.println(e.getMessage());
						}
			}
		return message;  
		}
	//Delete from DB
	public String delete(int key, String replicaId, String transactionId) {
		System.out.println("Inide DB, Request is for Delete");
		String message =null;
		String value = "null";
		String oldValue= "null";
		try {
			Class.forName("org.sqlite.JDBC");
			connection = DriverManager.getConnection("jdbc:sqlite:assignment4"+replicaId+".db");
			System.out.println("DB Connection Opend successfully-- Delete");
			this.statement = connection.createStatement();
			ResultSet rs = statement.executeQuery("Select * from Balances where  accountNumber= "+key);
			while (rs.next()) {
					oldValue = rs.getString("amount");
			}
			String sql = "DELETE from Balances where accountNumber= "+key; 
			System.out.println("Query is  : "+sql);
		    statement.executeUpdate(sql);
		    System.out.println("Delete Successful");
		    message = "Record with key : "+key + " deleted Successfully";
		    writeArrivedLog(key, value, oldValue, "VOTE_COMMIT", "Delete", replicaId, transactionId);
		    //return message ;
		      
		} catch (SQLException e) {
			System.out.println("SQLException during delete operation :   "+e.getMessage());
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			System.out.println("ClassNotFoundException during delete operation :   "+e.getMessage());
		}catch (Exception e) {
			System.out.println("Exception in get key:   "+e.getMessage());
		}finally {
					try {
						statement.close();
						connection.close();
					} catch (SQLException e) {
						System.out.println(e.getMessage());
					}
		}
	return message; 
	}
	
/*	
	public void UpdateBackupTable(int key, String value, String oldValue, String state, String method) {
			
			String sql = "Insert into LoggingTable (accountNumber,amount, oldValue, state, method)"+" VALUES ("+key+","+value+","+oldValue+","+state+","+method+");";
			System.out.println("LOGGER--- "+sql);
			try {
				this.statement = connection.createStatement();
				statement.executeUpdate(sql);
			} catch (SQLException e) {
				e.printStackTrace();
			}
		    System.out.println("Insert in Logger DB Successfully");
			
	}*/
	
	public void writeArrivedLog(int key, String value, String oldValue, String state, String method, String replicaId,  String transactionId ) {
		System.out.println("Writing Arrived Log, arrivedLog "+replicaId+".txt");
		String mystring = state+" "+method+ " " + key+ " " +oldValue+ " "+ value + " " +transactionId  ;
		try {
            File log = new File("arrivedLog"+replicaId+".txt"); 
            if(!log.exists()){
            	log.createNewFile();
            	}
            BufferedWriter bw = new BufferedWriter(new FileWriter(log , true)); 
            bw.write(mystring+"\n");
           // bw.newLine();
            System.out.println(mystring); 
            bw.close();
        }catch (Exception ex) {
            System.out.println(ex.toString());
        }
	}
	
	public void writeCommitLog(int key, String value, String oldValue, String state, String method, String replicaId, String transactionId  ) {
		System.out.println("Writing Commit  Log, CommitedLog "+replicaId+".txt");
		String mystring = state+" "+method+ " " + key+ " " + transactionId+ " " +oldValue+ " "+ value ;
		try {
	        File log = new File ("CommitedLog"+replicaId+".txt");
            if(!log.exists()){
            	log.createNewFile();
            	}
            BufferedWriter bw = new BufferedWriter(new FileWriter(log , true)); 
            bw.write(mystring+"\n");
           // bw.newLine();
            System.out.println(mystring); 
            bw.close();
        }catch (Exception ex) {
            System.out.println(ex.toString());
        }
	}
	
	public void commit(String requestType, int key, String value, String replicaId, String transactionId) {
		System.out.println("Inide DB, Request is for Commit");
		writeCommitLog(key, value, "", "GLOBAL_COMMIT", requestType, replicaId, transactionId);
		
	}
	
	public void abort(String requestType, int key, String value, String replicaId) {
		System.out.println("Inide DB, Request is for Abort");
		if(requestType.equals("Delete")) {
			System.out.println("Abort for Delete--");
			try {
				FileInputStream fstream = new FileInputStream("arrivedLog"+replicaId+".txt");
				BufferedReader br = new BufferedReader(new InputStreamReader(fstream));
				String strLine;
				   /* read log line by line */
				   while ((strLine = br.readLine()) != null)   {
				     /* parse strLine to obtain what you want */
					 
					   System.out.println("Inside While-- Abort Delete");
					   System.out.println (strLine);
				     if(strLine.contains(Integer.toString(key)) && strLine.contains(value) &&
				    	 strLine.contains("Delete") && strLine.contains("VOTE_COMMIT") ) {
				    	 System.out.println (strLine);
				    	 String[] words = strLine.split("\\s+");
				    	 value = words[3];
				    	 System.out.println("Deleted Value was"+value);
				    	 for(int i=0;i<5;i++) {
				    		 System.out.println(words[i]);
				    	 }
				    	   try {
				    		   	Class.forName("org.sqlite.JDBC");
				    		   	connection = DriverManager.getConnection("jdbc:sqlite:assignment4"+replicaId+".db");
				    		   	System.out.println("DB Connection Opend successfully-- Abort");
								this.statement = connection.createStatement();
								String sql = "INSERT INTO Balances (accountNumber, amount) " +  "VALUES ("+key+","+value+" );"; 
								System.out.println("Query is  : "+sql);
							    statement.executeUpdate(sql); 
						    break;
						} catch (SQLException e) {
							System.out.println("SQLException during Abort, Delete operation :   "+e.getMessage());
						}
				     }
				   }
				   fstream.close();
				
				
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}  catch (ClassNotFoundException e) {
				System.out.println("ClassNotFoundException Exception Abort "+e.getMessage());
			}catch (Exception e) {
				System.out.println("Exception in get key:   "+e.getMessage());
			}finally {
						try {
							statement.close();
							connection.close();
						} catch (SQLException e) {
							System.out.println(e.getMessage());
						}
			}
		}
		
		else if(requestType.equals("Put")) {
			System.out.println("Abort for Put--");
			try {
				FileInputStream fstream = new FileInputStream("arrivedLog"+replicaId+".txt");
				BufferedReader br = new BufferedReader(new InputStreamReader(fstream));
				String strLine;
				
				   /* read log line by line */
				   while ((strLine = br.readLine()) != null)   {
				     /* parse strLine to obtain what you want */
					 System.out.println("Inside While-- Abort Put");
					 System.out.println (strLine);
				     if(strLine.contains(Integer.toString(key)) && strLine.contains(value) &&(
				    		strLine.contains("Insert") || strLine.contains("Update")) && strLine.contains("VOTE_COMMIT") ) {
				    	 
				    	 System.out.println (strLine);
				    	 String[] words = strLine.split("\\s+");
				    	 for(int i=0;i<5;i++) {
				    		 System.out.println(words[i]);
				    	 }
				    	 String method = words[1];
				    	 try {
				    		 	Class.forName("org.sqlite.JDBC");
				    		 	connection = DriverManager.getConnection("jdbc:sqlite:assignment4"+replicaId+".db");
				    		 	System.out.println("DB Connection Opend successfully-- Abort");
								this.statement = connection.createStatement();
						    	 if(method.equals("Insert")) {
						    		 String sql = "DELETE from Balances where accountNumber= "+key;  
						    		 System.out.println("Query is  : "+sql);
								    statement.executeUpdate(sql); 
								    break;
						    	 } else if (method.equals("Update")) {
						    		 String oldValue = words[3];
						    		 String sql = "UPDATE Balances SET amount = " +oldValue+ " where accountNumber = "+key ;
						    		 System.out.println("Query is  : "+sql);
									 statement.executeUpdate(sql); 
									  break;
						    	 }
						} catch (SQLException e) {
							System.out.println("SQLException during Abort, Put operation :   "+e.getMessage());
						}
				     }
				   }
				   fstream.close();
				
				
			} catch (FileNotFoundException e) {
				System.out.println("FileNotFoundException Abort "+e.getMessage());
			} catch (IOException e) {
				System.out.println("ClassNotFoundException Abort "+e.getMessage());
			} catch (ClassNotFoundException e) {
				System.out.println("ClassNotFoundException during delete operation :   "+e.getMessage());
			}catch (Exception e) {
				System.out.println("Exception in get key:   "+e.getMessage());
			}finally {
						try {
							statement.close();
							connection.close();
						} catch (SQLException e) {
							System.out.println(e.getMessage());
						}
			}
		}
		getAll();
	}
		public void getAll() {
			System.out.println("Get Alll");
					
					try {
						Class.forName("org.sqlite.JDBC");
						connection = DriverManager.getConnection("jdbc:sqlite:assignment4"+this.replicaId+".db");
						System.out.println("DB Connection Opend successfully-- Get");
						this.statement = connection.createStatement();
						
						ResultSet rs = statement.executeQuery("Select * from Balances ");
						int recordCount =0;
						while (rs.next()) {
							recordCount++;
							float amount = rs.getFloat("amount");
							System.out.println("Balance for account number "+rs.getInt("accountNumber")+" is $ "+amount);
						}
						
						System.out.println("Record count is "+ recordCount);
					} catch (SQLException e) {
						System.out.println("SQLException during delete operation :   "+e.getMessage());
					}catch (Exception e) {
						System.out.println("Exception in get All : "+e.getMessage());
					}finally {
						try {
							statement.close();
							connection.close();
						} catch (SQLException e) {
							System.out.println(e.getMessage());
						}
			}
			         
	}
		
	public void recoverDB() {
		
		System.out.println("************ Inside Recovery ****************");
		String arrivedTransactionId = null;
		String commitedTransactionId = null ;
		boolean isCommited = false ;
		
		try {
			FileInputStream arrivedFstream = new FileInputStream("arrivedLog"+replicaId+".txt");
			BufferedReader arrivedbr = new BufferedReader(new InputStreamReader(arrivedFstream));
			String arrivedStrLine;
			
			int count1 = 0 ;
			 while ((arrivedStrLine = arrivedbr.readLine()) != null)   {
			     /* parse strLine to obtain what you want */
				 System.out.println("\n");
				 System.out.println(++count1);
			  	 String[] words = arrivedStrLine.split("\\s+");
			  	 arrivedTransactionId =words [5];
			  	 int count = 0;

			  	System.out.println("Checking for "+arrivedTransactionId);
				FileInputStream CommitedFstream = new FileInputStream("CommitedLog"+replicaId+".txt");
				BufferedReader committedbr = new BufferedReader(new InputStreamReader(CommitedFstream));
				String commitedStrLine;
			  	 while((commitedStrLine = committedbr.readLine()) != null){
				  	 String[] commitedWords = commitedStrLine.split("\\s+");
				  	 commitedTransactionId =commitedWords [3];
				  	 System.out.println("Count "+ (++count) + commitedTransactionId);
				  	 if(arrivedTransactionId.equals(commitedTransactionId)) {
				  		 System.out.println("Inside EQUALS  " +arrivedTransactionId +"   "+ commitedTransactionId);
				  		 isCommited= true ;
				  		 break;
				  	 }
			  	 }
			  	committedbr.close();
			  	System.out.println("----------"+isCommited+ "for "+ arrivedTransactionId);
				if (!isCommited) {
						System.out.println("Transaction "+arrivedTransactionId+" is not in Commited log, we need to revert that entry, Processing......");
					try {
						String method = words[1];
						String value = words[3];
						String key = words[2];
						Class.forName("org.sqlite.JDBC");
						connection = DriverManager.getConnection("jdbc:sqlite:assignment4" + replicaId + ".db");
						this.statement = connection.createStatement();
						if (method.equals("Delete")) {
							System.out.println("Deleted data was key : " + key + " Value : " + value);
							try {
								String sql = "INSERT INTO Balances (accountNumber, amount) " + "VALUES (" + key + ","
										+ value + " );";
								System.out.println("Query is  : " + sql);
								statement.executeUpdate(sql);
								break;
							} catch (SQLException e) {
								System.out
										.println("SQLException during Recover, Delete operation :   " + e.getMessage());
							}
						} else if (method.equals("Insert")) {
							System.out.println("Inserted  data was key : " + key + " Value : " + value);
							try {
								String sql = "DELETE from Balances where accountNumber= " + key;
								System.out.println("Query is  : " + sql);
								statement.executeUpdate(sql);
							} catch (SQLException e) {
								System.out
										.println("SQLException during Recover, Insert operation :   " + e.getMessage());
							}
						} else if (method.equals("Update")) {
							String oldValue = words[3];
							System.out.println("Inserted  data was key : " + key + " Value : " + value);
							try {
								String sql = "UPDATE Balances SET amount = " + oldValue + " where accountNumber = "
										+ key;
								System.out.println("Query is  : " + sql);
								statement.executeUpdate(sql);
							} catch (SQLException e) {
								System.out
										.println("SQLException during Recover, Update operation :   " + e.getMessage());
							}
						}

					} catch (ClassNotFoundException e) {
						System.out.println("ClassNotFoundException : " + e.getMessage());
					} catch (SQLException e1) {
						System.out
								.println("SQLException during Recover (isCommited), operation :   " + e1.getMessage());
					} finally {
						try {
							statement.close();
							connection.close();
						} catch (SQLException e) {
							System.out.println(e.getMessage());
						}
					}

				}else {
					System.out.println("Transaction "+arrivedTransactionId+ " is committed ");
				}
			 }  
			 arrivedbr.close();
			 
			    
		} catch (FileNotFoundException e) {
			System.out.println("File NOt found exception inside Recovery-- "+e.getMessage());
		} catch (IOException e) {
			System.out.println("IO exception inside Recovery-- "+e.getMessage());
		}
	}
	/*
	public boolean getCount(int key) {
		boolean isAllowed = false ;
		try {
			System.out.println("Count");
			connection = DriverManager.getConnection("jdbc:sqlite:test_assignment1.db");
			this.statement = this.connection.createStatement();
			String sql = "select count(*) as total from Balances where accountNumber = "+key ;
			ResultSet rs= statement.executeQuery(sql)	;	
			
			int count = rs.getInt("total");
			System.out.println(count);
			if(count==0) {
				isAllowed = true ;
				System.out.println("New");
				
			}
			connection.close();
			System.out.println("Record Count is "+ count + " and is insertion allowed " +isAllowed);
		}catch (Exception e) {
			System.out.println("Get Count, Error: "+e.getMessage());
		}
		return isAllowed;
	}
	
	public static void main(String[] args) {
		Database db = new Database();
		//db.getAll();
		boolean isAllowed = db.getCount(78);
		if(isAllowed) {
			System.out.println("Entry nt PRESENT");
		}else {
			System.out.println("Entry already exist in DB");
		}
	}*/
}

