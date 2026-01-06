 package ai.sedn.unsupervised;

import java.sql.ResultSet;

import ai.sedn.plunijava.PlUniJava;

public class db_object {         
    public ResultSet R;
    public int Nc;
    public boolean array;
    public PlUniJava P;

    public db_object(ResultSet R, int Nc, boolean array) {         
        this.R = R;
        this.Nc = Nc;
        this.array = array;
     }

    public db_object(PlUniJava PL, int Nc, boolean array) {         
        this.P = PL;
        this.Nc = Nc;
        this.array = array;
    }
}

