package server;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.Objects;
import java.util.Scanner;
import java.util.function.Predicate;
import java.util.Random;
import com.google.gson.*;

/**
 * Contains global variables that are used across the other classes.
 * All globals are defined in the form of static variables.
 * Any class that extends this class gets access to all the global variables.
 */
class ClientGlobals {
  protected static String INITIAL_REQ = "Choose the options:\n1. Run Pre-defined data\n2. Run Custom data\nEnter the option: ";
  protected static String REQ_QUERY = "Choose Request:\n1. GET\n2. PUT\n3. DELETE\n4. Exit\nEnter Request: ";
  protected static String INVALID_KEY = "Error: Invalid key";
  protected static String QUERY_KEY = "Enter key: ";
  protected static String QUERY_VALUE = "Enter value to the key: ";
  protected static String INVALID_VALUE = "Error: Invalid value";
  protected static String FILE_NOT_FOUND = "Error: The file is not found in given path!";
  protected static String REMOTE_EXCEP = "Error: Exception in connecting to the server!";
  protected static String REQUEST_SERVER_ID = "Enter the replica:\n1. 1\n2. 2\n3. 3\n4. 4\n5. 5\n6. Select Random server\nEnter value: ";
}


/**
 * Acts as a client in Key-Value Store project.
 * - It provides option to perform 10 PUT, 15 GET and 5 DELETE operations.
 * - It also provides option to perform the above operations manually by selecting the replica server.
 */
public class KeyValueClient extends ClientGlobals {

  private JsonObject GetPost;
  private JsonObject Put;

  /**
   * Constructor that sets the values of the required variables.
   */
  public KeyValueClient() {
    try {
      JsonObject obj = (JsonObject) JsonParser.parseReader(new FileReader("./data.json"));
      this.GetPost = (JsonObject) obj.get("GET_CREATE");
      this.Put = (JsonObject) obj.get("PUT");
    } catch(FileNotFoundException fnfe) {
      System.out.println(FILE_NOT_FOUND);
    }
  }

  /**
   * Take number input from the user.
   * @param content Message to be displayed
   * @param constraint constraints of the taken input
   * @return the number inputted
   */
  public int numbInput(String content, Predicate<String> constraint) {
    Scanner s = new Scanner(System.in);
    String data = "";
    while(true) {
      System.out.print(content);
      data = s.nextLine();
      if(constraint.test(data)) {
        break;
      } else {
        System.out.println("Error: Enter a valid value!");
      }
    }
    return Integer.valueOf(data);
  }

  /**
   * Performs GET request to the server using Java RMI. The instance ID is either randomly created
   * or the user is given option to specify the instanceID.
   * @param key value of the key to be queried
   * @param iId value of the instance ID to perform the query upon
   * @throws NotBoundException accessing instance that is not available
   * @throws MalformedURLException if the queried URL is malformed
   */
  public void handleGETRequest(String key, int iId) throws NotBoundException, MalformedURLException{
    try {
      Pair<String, KVStoreInterface> kvs = this.getKeyValueStoreInstance(iId);
      String resp = kvs.t.get(key);
      this.log(kvs.k, "GET " + key, resp);
    } catch(RemoteException | InterruptedException ex) {
      this.log("N/A", "GET " + key, REMOTE_EXCEP);
    }
  }

  /**
   * Performs PUT request to the server using Java RMI. This is capable to perform both inserting
   * and updating a key-value pair.
   * @param key value of the key to inserted
   * @param value value to be inserted along with the key
   * @param iId value of the instance ID to perform the put upon
   * @throws NotBoundException accessing instance that is not available
   * @throws MalformedURLException if the queried URL is malformed
   */
  public void handlePUTRequest(String key, String value, int iId) throws NotBoundException, MalformedURLException {
    try {
      Pair<String, KVStoreInterface> kvs = this.getKeyValueStoreInstance(iId);
      String resp = kvs.t.put(key, value);
      this.log(kvs.k, "PUT " + key + ":" + value, resp);
    } catch(RemoteException | InterruptedException ex) {
      this.log("N/A", "PUT " + key + ":" + value, REMOTE_EXCEP);
    }
  }

  /**
   * Performs DELETE request to the server using Java RMI. This deletes the key from the key-value
   * store.
   * @param key value of the key to be deleted
   * @param iId value of the instance ID to perform the put upon
   * @throws NotBoundException accessing instance that is not available
   * @throws MalformedURLException if the queried URL is malformed
   */
  public void handleDELETERequest(String key, int iId) throws NotBoundException, MalformedURLException {
    try {
      Pair<String, KVStoreInterface> kvs = this.getKeyValueStoreInstance(iId);
      String resp = kvs.t.delete(key);
      this.log(kvs.k, "DELETE " + key, resp);
    } catch(RemoteException | InterruptedException ex) {
      this.log("N/A", "DELETE " + key, REMOTE_EXCEP);
    }
  }

  /**
   * Log the data to the client standard log.
   * @param server server to which request to be sent
   * @param req request to be sent to server
   * @param resp response generated by server
   */
  public void log(String server, String req, String resp) {
    System.out.println(System.currentTimeMillis() + " -- Server: " + server + " Request: " + req + " Response: " + resp);
  }

  /**
   * Take the string input from the user.
   * @param content Message to be displayed
   * @param errMsg error message to be displayed on an erreneous input
   * @return the inputted string
   */
  public String stringInput(String content, String errMsg) {
    Scanner s = new Scanner(System.in);
    String out = "";
    while(true) {
      System.out.print(content);
      out = s.nextLine();
      if(!Objects.equals(out, ""))
        break;
      else
        System.out.println(errMsg);
    }
    return out;
  }

  /**
   * Get the specified or random Instance Id from the registry.
   * @param id id of the instance to be brought
   * @return Pair of String and KeyValueStore instance
   * @throws NotBoundException accessing instance that is not available
   * @throws MalformedURLException if the queried URL is malformed
   * @throws RemoteException any communication related issues
   */
  private Pair<String, KVStoreInterface> getKeyValueStoreInstance(int id) throws NotBoundException, MalformedURLException, RemoteException {
    if(id == 6)
      return this.getKeyValueStoreInstance();
    else
      return new Pair<>("KVServer_" + id, ((KVStoreInterface)Naming.lookup("//localhost:500" + id + "/KVServer")));
  }

  /**
   * Get a random Instance Id from the registry.
   * @return Pair of String and KeyValueStore instance
   * @throws NotBoundException accessing instance that is not available
   * @throws MalformedURLException if the queried URL is malformed
   * @throws RemoteException any communication related issues
   */
  private Pair<String, KVStoreInterface> getKeyValueStoreInstance() throws NotBoundException, MalformedURLException, RemoteException {
    Random r = new Random(System.currentTimeMillis());
    int randV = 5001 + r.nextInt(5);
    return new Pair<>("KVServer_" + Integer.toString(randV), ((KVStoreInterface)Naming.lookup("//localhost:" + randV + "/KVServer")));
  }

  /**
   * Main method that initiates the client. This provides two options to the users:
   * - Run the default set of key-value pairs.
   * - Provide a manual way to perform GET, PUT and DELETE operations. The user is given choice
   * to perform these operations.
   * @param args pass any command-line arguments.
   */
  public static void main(String[] args) {
    try {
      KeyValueClient kvc = new KeyValueClient();
      int data = kvc.numbInput(INITIAL_REQ, p -> Objects.equals(p, "1") || Objects.equals(p, "2"));
      if(data == 1) {
        kvc.runDefaults();
      } else {
        while(true) {
          int req = kvc.numbInput(REQ_QUERY, p -> Objects.equals(p, "1") || Objects.equals(p, "2") || Objects.equals(p, "3") || Objects.equals(p, "4"));
          int instanceID = 6;
          if(req != 4)
            instanceID = kvc.numbInput(REQUEST_SERVER_ID, p -> Objects.equals(p, "1") || Objects.equals(p, "2") || Objects.equals(p, "3") || Objects.equals(p, "4") || Objects.equals(p, "5") || Objects.equals(p, "6"));
          switch(req) {
            case 1:
              kvc.handleGETRequest(kvc.stringInput(QUERY_KEY, INVALID_KEY), instanceID);
              break;
            case 2:
              kvc.handlePUTRequest(kvc.stringInput(QUERY_KEY, INVALID_KEY), kvc.stringInput(QUERY_VALUE, INVALID_VALUE), instanceID);
              break;
            case 3:
              kvc.handleDELETERequest(kvc.stringInput(QUERY_KEY, INVALID_KEY), instanceID);
              break;
            case 4:
              System.exit(0);
            default:
              System.out.println("Invalid option!");
          }
        }
      }
    } catch(NotBoundException | MalformedURLException nb) {
      System.out.println(nb);
      System.out.println(REMOTE_EXCEP);
    }
  }

  /**
   * Run the default set of key-value pairs.
   * @throws NotBoundException accessing instance that is not available
   * @throws MalformedURLException if the queried URL is malformed
   */
  public void runDefaults() throws NotBoundException, MalformedURLException{
    // Post the requests first
    for(String i : this.GetPost.keySet()) {
      this.handlePUTRequest(i, this.GetPost.get(i).getAsString(), 6);
    }

    // Get the Created entries and check
    for(String i : this.GetPost.keySet()) {
      this.handleGETRequest(i, 6);
    }

    for(String i : this.Put.keySet()) {
      this.handleDELETERequest(i, 6);
    }


    for(String i : this.GetPost.keySet()) {
      this.handleGETRequest(i, 6);
    }
  }
}
