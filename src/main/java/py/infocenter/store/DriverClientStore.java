
package py.infocenter.store;

import com.google.common.collect.Multimap;
import java.util.List;
import py.icshare.DriverClientInformation;
import py.icshare.DriverClientKey;


public interface DriverClientStore {

  public DriverClientInformation getLastTimeValue(DriverClientKey driverClientKey);

  public List<DriverClientInformation> list();

  public Multimap<DriverClientKey, DriverClientInformation> listDriverKey();

  public void loadToMemory();

  public void delete(DriverClientKey driverClientKey);

  public void deleteValue(DriverClientInformation driverClientInformation);

  public void save(DriverClientInformation driverClientInformation);

  public void clearMemoryData();
}
