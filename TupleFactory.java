package IP;

import java.text.SimpleDateFormat;

public class TupleFactory {
    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

    public static CustomTuple createFromCustomer(int c_CUSTKEY, String c_MKTSEGMENT) {
        CustomTuple customTuple = new CustomTuple();
        customTuple.C_CUSTKEY = c_CUSTKEY;
        customTuple.C_MKTSEGMENT = c_MKTSEGMENT;
        customTuple.type = CustomTuple.CUSTOMER;

        return customTuple;
    }

    public static CustomTuple createFromOrder(int o_OREDERKEY, int o_CUSTKEY, String o_ORDERDATE, int o_SP) throws Exception{
        CustomTuple customTuple = new CustomTuple();
        customTuple.O_CUSTKEY = o_CUSTKEY;
        customTuple.O_OREDERKEY = o_OREDERKEY;
        customTuple.O_SP = o_SP;
        customTuple.O_ORDERDATE = sdf.parse(o_ORDERDATE);
        customTuple.type = CustomTuple.ORDER;

        return customTuple;
    }

    public static CustomTuple createFromLineItem(int l_ORDERKEY, double l_EP, double l_DIS, String l_SHIPDATE) throws Exception {
        CustomTuple customTuple = new CustomTuple();
        customTuple.L_DIS = l_DIS;
        customTuple.L_EP = l_EP;
        customTuple.L_ORDERKEY = l_ORDERKEY;
        customTuple.L_SHIPDATE = sdf.parse(l_SHIPDATE);
        customTuple.type = CustomTuple.LINEITEM;

        return customTuple;
    }
}
