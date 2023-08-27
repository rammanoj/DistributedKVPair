package server;

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * The ProposerInterface provides a remote method to initiate a proposal in the Paxos consensus algorithm.
 * It is part of the Paxos distributed consensus protocol, representing the proposing role.
 */
public interface ProposerInterface extends Remote {

  /**
   * Initiates a proposal with the given proposal ID and value.
   *
   * @param proposalId The unique identifier for the proposal.
   * @param proposalValue The value being proposed.
   * @throws RemoteException If a remote invocation error occurs.
   */
  boolean propose(String proposalId, Object proposalValue) throws RemoteException, InterruptedException;
}
