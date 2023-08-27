package server;

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * A class that manages get, put and delete operations onto the key-value store.
 */
public interface KVStoreInterface extends Remote {

  /**
   * insert or update a value into the key-value store.
   * @param key key to be inserted.
   * @param value value to be inserted
   * @return response if the value is successfully updated
   * @throws RemoteException if any issue in connecting to server
   * @throws InterruptedException if sleep is interrupted
   */
  String put(String key, String value) throws RemoteException, InterruptedException;

  /**
   * Delete a value from the key-value store.
   * @param key key to be deleted
   * @return response if the value is successfully deleted
   * @throws RemoteException if any issue in connecting to server
   * @throws InterruptedException if sleep is interrupted
   */
  String delete(String key) throws RemoteException, InterruptedException;

  /**
   * Get a value to a key from the key-value store.
   * @param key key to be inserted
   * @return value with respect to the key in the key-value store
   * @throws RemoteException if any issue in connecting to server
   * @throws InterruptedException if sleep is interrupted
   */
  String get(String key) throws RemoteException, InterruptedException;
}

