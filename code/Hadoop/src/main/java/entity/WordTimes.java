package entity;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class WordTimes implements WritableComparable<WordTimes> {
    public int upflow;
    public int downflow;
    public int allflow;
    public String phonenum="";

    public void write(DataOutput dataOutput) throws IOException {
         dataOutput.writeInt(upflow);
         dataOutput.writeInt(downflow);
         dataOutput.writeInt(allflow);
         dataOutput.writeUTF(phonenum);
    }

    @Override
    public String toString() {
        return "WordTimes{" +
                "upflow=" + upflow +
                ", downflow=" + downflow +
                ", allflow=" + allflow +
                ", phonenum='" + phonenum + '\'' +
                '}';
    }

    public WordTimes(int upflow, int downflow, int allflow, String phonenum) {
        this.upflow = upflow;
        this.downflow = downflow;
        this.allflow = allflow;
        this.phonenum = phonenum;
    }

    public WordTimes() {
        super();

    }

    public void readFields(DataInput dataInput) throws IOException {
         this.upflow=dataInput.readInt();
         this.downflow=dataInput.readInt();
         this.allflow=dataInput.readInt();
         this.phonenum=dataInput.readUTF();

    }

    public int getUpflow() {
        return upflow;
    }

    public void setUpflow(int upflow) {
        this.upflow = upflow;
    }

    public int getDownflow() {
        return downflow;
    }

    public void setDownflow(int downflow) {
        this.downflow = downflow;
    }

    public int getAllflow() {
        return allflow;
    }

    public void setAllflow(int allflow) {
        this.allflow = allflow;
    }

    public String getPhonenum() {
        return phonenum;
    }

    public void setPhonenum(String phonenum) {
        this.phonenum = phonenum;
    }

    @Override
    public int compareTo(WordTimes o) {
        if (o.allflow==this.allflow)
        {
            if(o.downflow>this.downflow)
            {
                return o.downflow-this.downflow;
            }
            else
                return o.allflow-this.downflow;
        }
        else
            return o.allflow-this.allflow;
    }
}
