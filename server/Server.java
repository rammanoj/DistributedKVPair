package server;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


/**
 * A global interface that contains all the global variables to used in the server class.
 */
interface Globals {
  public static int PROPOSER_TIMEOUT=100;
  public static int MAJORITY_CNT=3;
  public static int ACCEPTOR_DOWN_TIME=60;
}


/**
 * Implementation of a Server class that represents a node in a Paxos distributed consensus system.
 * This server plays the role of Proposer, Acceptor, and Learner in the Paxos algorithm, and it also handles key-value store operations.
 */
public class Server extends UnicastRemoteObject implements ProposerInterface, AcceptorInterface, LearnerInterface, KVStoreInterface, Globals  {
  private ConcurrentHashMap<String, String> kvStore = new ConcurrentHashMap<>();
  private Map<String, Pair<String, Operation>> log;
  private AcceptorInterface[] acceptors;
  private LearnerInterface[] learners;
  private int numServers;
  private int serverId;
  private Map<String, Pair<Integer, Boolean>> lrnCnt;

  private boolean acceptorDown = false;
  private long acceptorDownTime = 0;
  private Map<String, Boolean> outData;

  /**
   * Set an acceptor to Down and note down at what time did the acceptor went down.
   */
  public void setAcceptorDownToTrue() {
    this.acceptorDown = true;
    this.acceptorDownTime = System.currentTimeMillis() / 1000L;
  }

  /**
   * Constructor to create a Server instance.
   * @param serverId The unique ID of this server.
   * @param numServers The total number of servers in the system.
   */
  public Server(int serverId, int numServers) throws RemoteException {
    this.numServers = numServers;
    this.serverId = serverId;
    this.log = new HashMap<>();
    this.lrnCnt = new HashMap<>();
    this.outData = new HashMap<>();
  }

  /**
   * Set the acceptors for this server.
   * @param acceptors Array of acceptors.
   */
  public void setAcceptors(AcceptorInterface[] acceptors) throws RemoteException {
    this.acceptors = acceptors;
  }

  /**
   * Log all the requests received and the responses to the requests to the server STDOUT.
   * @param req request received by the server
   * @param resp response sent by the server
   */
  private void log(String req, String resp) throws RemoteException {
    System.out.println(System.currentTimeMillis() + " -- Server:" + this.serverId + " Request: " + req + " Response: " + resp);
  }

  /**
   * Set the learners for this server.
   * @param learners Array of learners.
   */
  public void setLearners(LearnerInterface[] learners) throws RemoteException {
    this.learners = learners;
  }

  /**
   * insert or update a value into the key-value store.
   * @param key key to be inserted.
   * @param value value to be inserted
   * @return response if the value is successfully updated
   * @throws RemoteException if any issue in connecting to server
   * @throws InterruptedException if sleep is interrupted
   */
  @Override
  public synchronized String put(String key, String value)
      throws RemoteException, InterruptedException {
    if(proposeOperation(new Operation("PUT", key, value)))
      return "Successfully inserted/updated the value";
    else
      return "Exception in inserting/updating the value";
  }

  /**
   * Delete a value from the key-value store.
   * @param key key to be deleted
   * @return response if the value is successfully deleted
   * @throws RemoteException if any issue in connecting to server
   * @throws InterruptedException if sleep is interrupted
   */
  @Override
  public synchronized String delete(String key) throws RemoteException, InterruptedException {
    if(proposeOperation(new Operation("DELETE", key, null)))
      return "Successfully deleted the value";
    else
      return "Exception in deleting the value";
  }

  /**
   * Get a value to a key from the key-value store.
   * @param key key to be inserted
   * @return value with respect to the key in the key-value store
   * @throws RemoteException if any issue in connecting to server
   */
  @Override
  public synchronized  String get(String key) throws RemoteException {
    String out = kvStore.getOrDefault(key, "Key does not exist to return");
    this.log("GET: " + key, out);
    return out;
  }

  /**
   * Propose an operation to be applied.
   * @param operation The operation to be proposed.
   * @throws RemoteException If a remote error occurs.
   */
  private boolean proposeOperation(Operation operation) throws RemoteException, InterruptedException {
    String proposalId = generateProposalId();
    return propose(proposalId, operation);
  }

  /**
   * Check if acceptor is down. Return a boolean value depending on the acceptor status.
   *
   * @return true if the acceptor is down
   */
  private boolean isAcceptorDown() throws RemoteException {
    if(this.acceptorDown) {
      long currentTime = System.currentTimeMillis() / 1000L;
      if(this.acceptorDownTime + ACCEPTOR_DOWN_TIME <= currentTime) {
        System.out.println(System.currentTimeMillis() + " -- Acceptor " + this.serverId + " restarted!");
        this.acceptorDown = false;
        this.acceptorDownTime = 0;
        return false;
      }
      return true;
    }
    return false;
  }

  /**
   * Process the prepare operation of a acceptor. Receive the prepare request from the acceptor
   * and accept / reject it based on the if there's any latest operation in it's log.
   * @param proposalId The unique ID of the proposal.
   * @param oper operation to be performed
   * @return pair if  the given operation is accepted or not along with accepted operation
   * @throws RemoteException if there's any issue with RMI
   */
  @Override
  public synchronized Pair<Boolean, Operation> prepare(String proposalId, Object oper) throws RemoteException {
    if(this.isAcceptorDown()) {
      return null;
    }
    // Implement Paxos prepare logic here
    Operation op = (Operation)oper;

    // check in the log for any highest value.
    if(this.log.containsKey(op.key)) {
      if(Long.parseLong(this.log.get(op.key).k.split(":")[1]) > Long.parseLong(proposalId.split(":")[1])) {
        return new Pair<>(false, op);
      }
    }
    this.log.put(op.key, new Pair<>(proposalId, op));
    return new Pair<>(true, op);
  }

  /**
   * Accept the value that the proposers give. If there's any operation with higher number, reject
   * the acceptance.
   * @param proposalId The unique ID of the proposal.
   * @param proposalValue The value of the proposal.
   * @throws RemoteException if issue arises with RMI
   */
  @Override
  public synchronized void accept(String proposalId, Object proposalValue) throws RemoteException {
    if(this.isAcceptorDown()) {
      return;
    }

    // Implement Paxos accept logic here
    Operation op = (Operation)proposalValue;

    // check in the log for any highest value.
    if(this.log.containsKey(op.key)) {
      if(Long.parseLong(this.log.get(op.key).k.split(":")[1]) <= Long.parseLong(proposalId.split(":")[1])) {
        for(int i=0; i<5; i++) {
          this.learners[i].learn(proposalId, proposalValue);
        }
      }
    }
  }

  /**
   * Porpose a value to all the acceptors and get their input on the proposal. If all the
   * acceptors accept the proposal, send a accept request. If any of them rejects it, send a
   * accept request for the higher operation value.
   * @param proposalId The unique identifier for the proposal.
   * @param proposalValue The value being proposed.
   * @return true/false based on if operation is successful or not.
   * @throws RemoteException if issue arises with RMI
   * @throws InterruptedException if sleep is interrupted
   */
  @Override
  public synchronized boolean propose(String proposalId, Object proposalValue)
      throws RemoteException, InterruptedException {
    // Implement Paxos propose logic here
    List<Pair<Boolean, Operation>> p = new ArrayList<>();
    for(int i=0; i<5; i++) {
      Pair<Boolean, Operation> pp = this.acceptors[i].prepare(proposalId, proposalValue);
      p.add(pp);
    }
    Thread.sleep(PROPOSER_TIMEOUT);
    int highestValue = -1, majorityCount = 0;

    // check for rejections and majority
    for(int i=0; i<5; i++) {
      if(p.get(i) != null) {
        if(!p.get(i).k)
          highestValue = i;
        else
          majorityCount += 1;
      }
    }

    // if any rejects, accept the highest value
    if(highestValue != -1) {
      for(int i=0; i<5; i++) {
        this.acceptors[i].accept(proposalId, p.get(i).t);
      }
      return false;
    }

    // if majority, accept the propsed value
    if(majorityCount >= MAJORITY_CNT) {
      for(int i=0; i<5; i++) {
        if(p.get(i) != null)
          this.acceptors[i].accept(proposalId, proposalValue);
      }
      while(!this.outData.containsKey(proposalId)) {
        Thread.sleep(100);
      }
      return this.outData.get(proposalId);
    }

    return false;
  }

  /**
   * learn the value that the acceptors pass.
   * @param proposalId The unique identifier for the proposal.
   * @param acceptedValue The value that has been accepted.
   * @throws RemoteException if any issue with the RMI
   */
  @Override
  public synchronized void learn(String proposalId, Object acceptedValue) throws RemoteException {
    // Implement Paxos learn logic here
    if(!this.lrnCnt.containsKey(proposalId)) {
      this.lrnCnt.put(proposalId, new Pair<>(1, false));
    } else {
      Pair<Integer, Boolean> p = this.lrnCnt.get(proposalId);
      p.k += 1;
      if(p.k >= MAJORITY_CNT && !p.t) {
        this.outData.put(proposalId, this.applyOperation((Operation) acceptedValue));
        p.t = true;
      }
      this.lrnCnt.put(proposalId, p);
    }
  }

  /**
   * Generates a unique proposal ID.
   * @return A unique proposal ID.
   */
  private String generateProposalId() throws RemoteException {
    // Placeholder code to generate a unique proposal ID
    return this.serverId + ":" + System.currentTimeMillis();
  }

  /**
   * Apply the given operation to the key-value store.
   * @param operation The operation to apply.
   */
  private boolean applyOperation(Operation operation) throws RemoteException {
    if (operation == null) return false;
    switch (operation.type) {
      case "PUT":
        kvStore.put(operation.key, operation.value);
        this.log("PUT " + operation.key + ":" + operation.value, "Successfully inserted/updated the key");
        return true;
      case "DELETE":
        if(kvStore.containsKey(operation.key)) {
          kvStore.remove(operation.key);
          this.log("DELETE " + operation.key, "Successfully deleted the key");
          return true;
        } else {
          this.log("DELETE " + operation.key, "Key does not exist to delete!");
          return false;
        }
      default:
        throw new IllegalArgumentException("Unknown operation type: " + operation.type);
    }
  }

}

/**
 * class representing an operation on the key-value store.
 */
class Operation {
  String type;
  String key;
  String value;

  Operation(String type, String key, String value) {
    this.type = type;
    this.key = key;
    this.value = value;
  }

  Operation(String type, String key) {
    this(type, key, null);
  }
}

/**
 * Create a Pair Object with any two generic types.
 * @param <K> Generic K that is used in pair creation
 * @param <T> Generic T that is used in pair creation
 */
class Pair<K, T> {
  T t;
  K k;

  Pair(K k, T t) {
    this.k = k;
    this.t = t;
  }

  Pair() {}
}
