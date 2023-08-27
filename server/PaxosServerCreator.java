package server;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * The PaxosServerCreator class is responsible for creating and binding the Paxos servers
 * within the RMI registry. It also configures the acceptors and learners for each server.
 */
public class PaxosServerCreator {

  private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

  /**
   * Create a scheduler that drops servers at random time.
   * @param servers list of servers to be dropped
   */
  private static void startRandomizedLoop(Server[] servers) {
    // Schedule a task to run at a random delay, then repeat every X seconds
    scheduler.scheduleWithFixedDelay(() -> PaxosServerCreator.dropServer(servers), 20, 500, TimeUnit.SECONDS);
  }

  /**
   * drop a server at random or just ignore when triggered.
   * @param s list of servers to be dropped
   */
  private static void dropServer(Server[] s)  {
    Random r = new Random();
    int v = r.nextInt(10);
    if(v < 5) {
      s[v].setAcceptorDownToTrue();
      System.out.println(System.currentTimeMillis() + " -- Acceptor " + v + " is down!");
    }
  }

  /**
   * The main method to launch the creation and binding process of the Paxos servers.
   *
   * @param args Command-line arguments (unused in this context).
   */
  public static void main(String[] args) {
    try {
      int numServers = 5; // Total number of servers
      int basePort = 5001; // Starting port number

      Server[] servers = new Server[numServers];
      startRandomizedLoop(servers);
      // Create and bind servers
      for (int serverId = 0; serverId < numServers; serverId++) {
        int port = basePort + serverId; // Increment port for each server
        // Create RMI registry at the specified port
        LocateRegistry.createRegistry(port);

        // Create server instance
        servers[serverId] = new Server(serverId, numServers);

        // Bind the server to the RMI registry
        Registry registry = LocateRegistry.getRegistry(port);
        registry.rebind("KVServer", servers[serverId]);

        System.out.println("Server " + serverId + " is ready at port " + port);
      }

      // Set acceptors and learners for each server
      for (int serverId = 0; serverId < numServers; serverId++) {
        AcceptorInterface[] acceptors = new AcceptorInterface[numServers];
        LearnerInterface[] learners = new LearnerInterface[numServers];
        for (int i = 0; i < numServers; i++) {
            acceptors[i] = servers[i];
            learners[i] = servers[i];
        }
        servers[serverId].setAcceptors(acceptors);
        servers[serverId].setLearners(learners);
      }

    } catch (Exception e) {
      System.err.println("Server exception: " + e.toString());
      e.printStackTrace();
    }
  }
}
