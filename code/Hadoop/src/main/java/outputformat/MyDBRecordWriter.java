package outputformat;

import entity.Flow;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Created by 代俊朴 on 2018/1/23.
 */
public class MyDBRecordWriter extends RecordWriter<Flow,NullWritable> {
    private static Connection conn;
    private static PreparedStatement ps;
    static{
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        try {
           conn=DriverManager.getConnection("jdbc:mysql://localhost:3306/TestHadoop","root","123");
           ps=conn.prepareStatement("INSERT INTO PhoneFlow (PhoneNumber, UpFlow, DownFlow,AllFlow) VALUE (?,?,?,?)");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    @Override
    public void write(Flow key, NullWritable value) throws IOException, InterruptedException {
        try {
            ps.setString(1,key.getPhoneNumber());
            ps.setInt(2,key.getUpFlow());
            ps.setInt(3,key.getDownFlow());
            ps.setInt(4,key.getAllFlow());
            ps.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        try {
            ps.close();
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
