package com.rao.study.testReducer;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OrderBean implements Writable {
    private String pname;
    private Float amount;

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(pname);
        out.writeFloat(amount);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.pname = in.readUTF();
        this.amount = in.readFloat();
    }


    public String getPname() {
        return pname;
    }

    public void setPname(String pname) {
        this.pname = pname;
    }

    public Float getAmount() {
        return amount;
    }

    public void setAmount(Float amount) {
        this.amount = amount;
    }
}
