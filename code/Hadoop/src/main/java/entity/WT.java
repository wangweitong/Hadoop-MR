package entity;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class WT implements WritableComparable<WT> {
    public WT() {
        super();
    }

    public WT(String word, int times) {

        this.word = word;
        this.times = times;
    }



    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public int getTimes() {
        return times;
    }

    public void setTimes(int times) {
        this.times = times;
    }

    public String word="";
    public int times;



    public void write(DataOutput dataOutput) throws IOException {
          dataOutput.writeUTF(word);
          dataOutput.writeInt(times);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.word=dataInput.readUTF();
        this.times=dataInput.readInt();
    }

    @Override
    public String toString() {
        return "WT{" +
                "word='" + word + '\'' +
                ", times=" + times +
                '}';
    }

    public int compareTo(WT o) {
        return o.times-this.times;
    }
}
