package outputformat;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class ReRecordWriter extends RecordWriter<Text,IntWritable> {
    private static Connection conn;
    private PreparedStatement ps;
    static {
        try {
            Class.forName("com.mysql.jdbc.Driver");
            conn= DriverManager.getConnection("jdbc:mysql://localhost:3306/TestHadoop","root","123");
            conn.prepareStatement("INSERT INTO WordCount(单词,次数) VALUES (?,?)");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    @Override
    public void write(Text key, IntWritable value) throws IOException, InterruptedException {
        try {
            ps.setString(1,key.toString());
            ps.setInt(2,value.get());
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
