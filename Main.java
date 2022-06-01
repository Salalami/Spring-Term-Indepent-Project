/*
实现插入和删除操作，插入的数据均为主键不同的tuple
主键相同会引起问题（future direction）
 */

package IP;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import scala.Int;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class Main {
    private static String SEGMENT;
    private static Date DATE;
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

    public static void main(String[] args) throws Exception{
        DATE = sdf.parse("1995-03-15");
        SEGMENT = "BUILDING";
        ArrayList<CustomTuple> lst = new ArrayList<>();
        InputProcess.read(lst);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<CustomTuple> source = env.fromCollection(lst);

        // 首先从source中过滤出customer tuple，再把符合条件的tuple保存在LR中并向后发送，不符合条件的丢弃
        SingleOutputStreamOperator<CustomTuple> customer = source.filter((FilterFunction<CustomTuple>)
                        customTuple -> customTuple.type == CustomTuple.CUSTOMER)
                .keyBy(data -> true)
                .process(new CustProcessFunc());
//        customer.print("Customer: ");

        /*
         customer为stream1，source为stream2;
         将customer的结果和source中的order tuple合流。keyby后custkey相同的tuples会到同一个状态下进行处理。因为每个key都有单独的状态，所以逻辑
         变得容易处理了；
         对customer维护一个boolean的valuestate，若上游传来该custkey，则为true，否则为false；对order，维护两个list，一个LR，一个NR；
         来了order tuple后，若该tuple的日期在DATE之前，valuestate为true，将该tuple放入LR中，并向下游发送，否则放入NR中。
         来了新custkey时更新LR，清空NR，并向下游发送变为alive的tuple。
         */
        SingleOutputStreamOperator<CustomTuple> order = customer.connect(source.filter((FilterFunction<CustomTuple>)
                        customTuple -> customTuple.type == CustomTuple.ORDER))
                .keyBy((KeySelector<CustomTuple, Integer>) customTuple -> customTuple.C_CUSTKEY,
                        (KeySelector<CustomTuple, Integer>) customTuple -> customTuple.O_CUSTKEY)
                .process(new OrderProcessFunc());
//        order.print("order: ");

        /*
         order为stream1，source为stream2;
         将order tuple和source tuple合流，keyby后orderkey相同的tuples会到同一个状态下进行处理;
         对order维护一个customtuple的valuestate，存储上游传过来的order tuple；对lineItem，维护两个liststate，LR和NR；
         来了lineItem tuple后，若该tuple的日期在DATE之后，且valuestate不为null，将该tuple放入LR中，且将该tuple和orderkey相同的order
         tuple合并，发送到下游；否则放入NR中；
         来了新的orderkey时，从NR中取出orderkey相同的tuples，将这些tuple分别和order tuple 合并，发送到下游任务中，并更新LR
         */
        SingleOutputStreamOperator<CustomTuple> lineItem = order.connect(source.filter((FilterFunction<CustomTuple>)
                customTuple -> customTuple.type == CustomTuple.LINEITEM))
                .keyBy((KeySelector<CustomTuple, Integer>) customTuple -> customTuple.O_OREDERKEY,
                        (KeySelector<CustomTuple, Integer>) customTuple -> customTuple.L_ORDERKEY)
                .process(new LineItemProcessFunc());
//        lineItem.print("lineItem: ");

        /*
         以l_orderkey, o_orderdate, o_shippriority为键进行分区，每个区单独处理该键的值的变化；
         每个key中的维护两个状态，一个liststate存储总tuple，一个valuestate存储revenue;
         每来一个tuple，更新revenue，并向下游发送更新后的结果。
         只考虑插入情况不会有重复数据
         */
        SingleOutputStreamOperator<CustomTuple> agg = lineItem.keyBy(new KeySelector<CustomTuple, String>() {
                    @Override
                    public String getKey(CustomTuple tuple) {
                        return String.valueOf(tuple.L_ORDERKEY) + tuple.O_ORDERDATE.getTime() + tuple.O_SP;
                    }
                })
                .process(new AggProcessFunc());
//        agg.print("agg: ");

        /*
        将上游发送来的tuple去重后排序输出
         */
        agg.flatMap(new OutputFlatMap())
                .setParallelism(1)
                .print("result: ");

        JobExecutionResult result = env.execute();
        System.out.println("The job took " + result.getNetRuntime(TimeUnit.SECONDS) + "s to execute");
    }

    public static class CustProcessFunc extends KeyedProcessFunction<Boolean, CustomTuple, CustomTuple> {
        private ListState<CustomTuple> LR;
//        private ListState<CustomTuple> NR;

        @Override
        public void open(Configuration parameters) {
            LR = getRuntimeContext().getListState(new ListStateDescriptor<>("customer-LR", Types.POJO(CustomTuple.class)));
//            NR = getRuntimeContext().getListState(new ListStateDescriptor<>("customer-NR", Types.POJO(CustomTuple.class)));
        }

        @Override
        public void processElement(CustomTuple tuple, KeyedProcessFunction<Boolean, CustomTuple, CustomTuple>.Context context, Collector<CustomTuple> collector) throws Exception {
            if(Objects.equals(tuple.C_MKTSEGMENT, SEGMENT)) {
                LR.add(tuple);
                collector.collect(tuple);
            }
        }
    }

    public static class OrderProcessFunc extends CoProcessFunction<CustomTuple, CustomTuple, CustomTuple> {
        private ListState<CustomTuple> LR;
        private ListState<CustomTuple> NR;
        private ValueState<Boolean> customerKey;

        @Override
        public void open(Configuration parameters) throws IOException {
            LR = getRuntimeContext().getListState(new ListStateDescriptor<>("order-LR", CustomTuple.class));
            NR = getRuntimeContext().getListState(new ListStateDescriptor<>("order-NR", CustomTuple.class));
            customerKey = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("order-CUSTKEY", Boolean.class));
        }

        // customer tuple
        @Override
        public void processElement1(CustomTuple customTuple, CoProcessFunction<CustomTuple, CustomTuple, CustomTuple>.Context context, Collector<CustomTuple> collector) throws Exception {
            customerKey.update(true);
            for (CustomTuple next : NR.get()) {
                LR.add(next);
                collector.collect(next);
            }
            NR.clear();
        }

        // order tuple
        @Override
        public void processElement2(CustomTuple customTuple, CoProcessFunction<CustomTuple, CustomTuple, CustomTuple>.Context context, Collector<CustomTuple> collector) throws Exception {
            if(customTuple.O_ORDERDATE.before(DATE)) {
                if(customerKey.value() != null) {
                    LR.add(customTuple);
                    collector.collect(customTuple);
                }
                else {
                    NR.add(customTuple);
                }
            }
        }

//        private void updateMapState(CustomTuple tuple, int mode) throws Exception{
//            MapState<Integer, List<CustomTuple>> tmp;
//            if(mode == 0) tmp = LR;
//            else tmp = NR;
//            if(tmp.contains(tuple.O_CUSTKEY)) {
//                tmp.get(tuple.O_CUSTKEY).add(tuple);
//            }
//            else {
//                ArrayList<CustomTuple> list = new ArrayList<>();
//                list.add(tuple);
//                tmp.put(tuple.O_CUSTKEY, list);
//            }
//        }
    }

    public static class LineItemProcessFunc extends CoProcessFunction<CustomTuple, CustomTuple, CustomTuple> {
        private ListState<CustomTuple> LR;
        private ListState<CustomTuple> NR;
        private ValueState<CustomTuple> orderKey;

        @Override
        public void open(Configuration parameters) {
            LR = getRuntimeContext().getListState(new ListStateDescriptor<>("lineItem-LR", CustomTuple.class));
            NR = getRuntimeContext().getListState(new ListStateDescriptor<>("lineItem-NR", CustomTuple.class));
            orderKey = getRuntimeContext().getState(new ValueStateDescriptor<>("lineItem-OK", CustomTuple.class));
        }

        // order
        @Override
        public void processElement1(CustomTuple tuple, CoProcessFunction<CustomTuple, CustomTuple, CustomTuple>.Context context, Collector<CustomTuple> collector) throws Exception {
            orderKey.update(tuple);
            for(CustomTuple c : NR.get()) {
                LR.add(c);
                collector.collect(mergeTuple(tuple, c));
            }
            NR.clear();
        }

        // lineItem
        @Override
        public void processElement2(CustomTuple tuple, CoProcessFunction<CustomTuple, CustomTuple, CustomTuple>.Context context, Collector<CustomTuple> collector) throws Exception {
            if(tuple.L_SHIPDATE.after(DATE)) {
                if(orderKey.value() != null) {
                    LR.add(tuple);
                    collector.collect(mergeTuple(orderKey.value(), tuple));
                }
                else {
                    NR.add(tuple);
                }
            }
        }

        private CustomTuple mergeTuple(CustomTuple order, CustomTuple lineItem) {
            lineItem.O_SP = order.O_SP;
            lineItem.O_ORDERDATE = order.O_ORDERDATE;
            lineItem.O_CUSTKEY = order.O_CUSTKEY;
            lineItem.O_OREDERKEY = order.O_OREDERKEY;

            return lineItem;
        }

//        private void updateMapState(CustomTuple tuple, int mode) throws Exception{
//            MapState<Integer, List<CustomTuple>> tmp;
//            if(mode == 0) tmp = LR;
//            else tmp = NR;
//
//            if(tmp.contains(tuple.L_ORDERKEY)) {
//                tmp.get(tuple.L_ORDERKEY).add(tuple);
//            }
//            else {
//                ArrayList<CustomTuple> list = new ArrayList<>();
//                list.add(tuple);
//                tmp.put(tuple.L_ORDERKEY, list);
//            }
//        }
    }

    public static class AggProcessFunc extends KeyedProcessFunction<String, CustomTuple, CustomTuple>{
        private ListState<CustomTuple> tuples;
        private ValueState<Double> revenue;

        @Override
        public void open(Configuration parameters) {
            tuples = getRuntimeContext().getListState(new ListStateDescriptor<>("agg-list", CustomTuple.class));
            revenue = getRuntimeContext().getState(new ValueStateDescriptor<>("agg-rev", Double.class));
        }

        @Override
        public void processElement(CustomTuple tuple, KeyedProcessFunction<String, CustomTuple, CustomTuple>.Context context, Collector<CustomTuple> collector) throws Exception {
            tuples.add(tuple);
            double rev;
            if(revenue.value() == null) {
                rev = tuple.L_EP * (1 - tuple.L_DIS);
                revenue.update(rev);
            }
            else {
                rev = revenue.value();
                rev += tuple.L_EP * (1 - tuple.L_DIS);
                revenue.update(rev);
            }
            tuple.revenue = rev;
            collector.collect(tuple);
        }
    }

    public static class OutputFlatMap extends RichFlatMapFunction<CustomTuple, String> {
        private static final ArrayList<CustomTuple> arr = new ArrayList<>();
        private static final HashMap<String, CustomTuple> map = new HashMap<>();


        @Override
        public void flatMap(CustomTuple tuple, Collector<String> collector) {
            String key = String.valueOf(tuple.L_ORDERKEY) + tuple.O_ORDERDATE.getTime() + tuple.O_SP;
            map.put(key, tuple);
            arr.clear();
            arr.addAll(map.values());

            arr.sort(Comparator.comparing(CustomTuple::getRevenue).reversed().thenComparing(CustomTuple::getO_ORDERDATE));
            StringBuilder sb = new StringBuilder();
            for(int i = 0; i < arr.size(); i++) {
                if(i < 10) {
                    CustomTuple customTuple = arr.get(i);
                    String tmp = "l_orderkey: " + customTuple.L_ORDERKEY + "\t"
                            + "o_orderdate: " + new Timestamp(customTuple.getO_ORDERDATE()) + "\t"
                            + "o_shippriority: " + customTuple.O_SP + "\t"
                            + "revenue: " + String.format("%.2f", customTuple.revenue) + "\n";
                    sb.append(tmp);
                }
            }
//            System.out.println(map.size());
            collector.collect(sb.toString());
        }
    }
}
