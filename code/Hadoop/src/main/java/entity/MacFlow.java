package entity;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MacFlow implements WritableComparable<MacFlow> {
    public String mac="";
    public int flow=0;

    public MacFlow() {
        super();
    }

    public MacFlow(String mac, int flow) {

        this.mac = mac;
        this.flow = flow;
    }

    public String getMac() {

        return mac;
    }

    public void setMac(String mac) {
        this.mac = mac;
    }

    public int getFlow() {
        return flow;
    }

    public void setFlow(int flow) {
        this.flow = flow;
    }



    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(mac);
        dataOutput.writeInt(flow);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.mac=dataInput.readUTF();
        this.flow=dataInput.readInt();
    }



    @Override
    public int compareTo(MacFlow o) {
        return o.flow-this.flow;
    }
}
