
package py.infocenter.store;

import java.io.IOException;
import java.sql.Blob;
import java.sql.SQLException;
import java.util.List;

public interface TaskStore {

  public void saveTask(TaskRequestInfo taskRequestInfo) throws IOException;

  public List<TaskRequestInfo> listAllTask(int limit) throws Exception;

  public TaskRequestInfo getTaskById(long taskId)
      throws IOException, SQLException, ClassNotFoundException;

  public void deleteTaskById(long taskId);

  public int clearDb() throws Exception;

  public Blob createBlob(byte[] bytes);
}
