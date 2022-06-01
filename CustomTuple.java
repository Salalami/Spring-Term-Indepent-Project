package IP;

import java.text.SimpleDateFormat;
import java.util.Date;

public class CustomTuple {
    public static int CUSTOMER = 0;
    public static int ORDER = 1;
    public static int LINEITEM = 2;
    public static int INSERT = 3;
    public static int DELETE = 4;
    public int type = -1;

    public int C_CUSTKEY = -1;
    public String C_MKTSEGMENT = " ";

    public int L_ORDERKEY = -1;
//    public int L_LINENUMBER;
    public double L_EP = -1.0;
    public double L_DIS = -1.0;
    public Date L_SHIPDATE = null;

    public int O_OREDERKEY = -1;
    public int O_CUSTKEY = -1;
    public Date O_ORDERDATE = null;
    public int O_SP = -1;

    public double revenue = -1.0;

//    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

    public CustomTuple() {
    }

    public Long getO_ORDERDATE() {
        return O_ORDERDATE.getTime();
    }

    public double getRevenue() {
        return revenue;
    }

    @Override
    public String toString() {
        return "CustomTuple{" +
                "type=" + type +
                ", C_CUSTKEY=" + C_CUSTKEY +
                ", C_MKTSEGMENT='" + C_MKTSEGMENT + '\'' +
                ", L_ORDERKEY=" + L_ORDERKEY +
                ", L_EP=" + L_EP +
                ", L_DIS=" + L_DIS +
                ", L_SHIPDATE=" + L_SHIPDATE +
                ", O_OREDERKEY=" + O_OREDERKEY +
                ", O_CUSTKEY=" + O_CUSTKEY +
                ", O_ORDERDATE=" + O_ORDERDATE +
                ", O_SP=" + O_SP +
                ", revenue=" + revenue +
                '}';
    }
}
