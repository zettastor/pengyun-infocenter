
package py.infocenter.store;

import java.util.List;
import py.icshare.StorageInformation;

/**
 * store information about the storage node.
 *
 */
public interface StorageDbStore {

  public void saveToDb(StorageInformation storageInformation);

  public void updateToDb(StorageInformation storageInformation);

  public StorageInformation getByInstanceIdFromDb(long instanceId);

  public List<StorageInformation> listFromDb();

  public int deleteFromDb(long instanceId);
}
