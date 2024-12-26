
package py.infocenter.store.control;


public interface VolumeJobStore {

  public boolean processCreateVolumeRequest();

  public void processDeleteVolumeRequest();
}
