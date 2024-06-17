

package ai.sedn.unsupervised;

import java.sql.ResultSet;

/**
*
* Helper-class to return ResultSet and number of columns together
* @author krefl
*
*/
public class db_object {         
    public ResultSet R;
    public int Nc;
    public boolean array;
    public Moonshot M;
    
    public db_object(ResultSet R, int Nc,boolean array) {         
        this.R = R;
        this.Nc = Nc;
        this.array = array;
     }

    public db_object(Moonshot MS, int Nc,boolean array) {         
        this.M = MS;
        this.Nc = Nc;
        this.array = array;
     }

}

