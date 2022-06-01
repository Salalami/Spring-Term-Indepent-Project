package IP;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;

public class InputProcess {
    public static void main(String[] args) {
        ArrayList<CustomTuple> lst = new ArrayList<>();

        read(lst);
        System.out.println(lst.size());
    }

    public static void read(ArrayList<CustomTuple> lst) {
        readCustomer(lst);
        readOrder(lst);
        readLineItem(lst);
    }

    public static void readCustomer(ArrayList<CustomTuple> lst){
        String path = "C:\\Users\\Kan\\Desktop\\bdt\\Spring Term\\ip\\dataset\\customer.txt";
        try {
            String encoding="UTF-8";
            File file = new File(path);
            if(file.isFile() && file.exists()){ //判断文件是否存在
                InputStreamReader read = new InputStreamReader(
                        new FileInputStream(file),encoding);//考虑到编码格式
                BufferedReader bufferedReader = new BufferedReader(read);
                String lineTxt;
//                int cnt = 0;
                while((lineTxt = bufferedReader.readLine()) != null){
                    String[] tmp = lineTxt.split("\\|");
                    lst.add(TupleFactory.createFromCustomer(Integer.parseInt(tmp[0]), tmp[6]));
//                    cnt++;
//                    if (cnt > 10) break;
                }
                read.close();
            }else{
                System.out.println("找不到指定的文件");
            }
        } catch (Exception e) {
            System.out.println("读取文件内容出错");
            e.printStackTrace();
        }
    }

    public static void readOrder(ArrayList<CustomTuple> lst){
        String path = "C:\\Users\\Kan\\Desktop\\bdt\\Spring Term\\ip\\dataset\\orders.txt";
        try {
            String encoding="UTF-8";
            File file = new File(path);
            if(file.isFile() && file.exists()){ //判断文件是否存在
                InputStreamReader read = new InputStreamReader(
                        new FileInputStream(file),encoding);//考虑到编码格式
                BufferedReader bufferedReader = new BufferedReader(read);
                String lineTxt;
//                int cnt = 0;
                while((lineTxt = bufferedReader.readLine()) != null){
                    String[] tmp = lineTxt.split("\\|");
//                    System.out.println(Arrays.toString(tmp));
                    lst.add(TupleFactory.createFromOrder(Integer.parseInt(tmp[0]), Integer.parseInt(tmp[1]), tmp[4],
                            Integer.parseInt(tmp[7])));
//                    cnt++;
//                    if(cnt > 10) break;
                }
                read.close();
            }else{
                System.out.println("找不到指定的文件");
            }
        } catch (Exception e) {
            System.out.println("读取文件内容出错");
            e.printStackTrace();
        }
    }

    public static void readLineItem(ArrayList<CustomTuple> lst){
        String path = "C:\\Users\\Kan\\Desktop\\bdt\\Spring Term\\ip\\dataset\\lineitem.txt";
        try {
            String encoding="UTF-8";
            File file = new File(path);
            if(file.isFile() && file.exists()){ //判断文件是否存在
                InputStreamReader read = new InputStreamReader(
                        new FileInputStream(file),encoding);//考虑到编码格式
                BufferedReader bufferedReader = new BufferedReader(read);
                String lineTxt;
                while((lineTxt = bufferedReader.readLine()) != null){
                    String[] tmp = lineTxt.split("\\|");
                    lst.add(TupleFactory.createFromLineItem(Integer.parseInt(tmp[0]), Double.parseDouble(tmp[5]),
                            Double.parseDouble(tmp[6]), tmp[10]));
                }
                read.close();
            }else{
                System.out.println("找不到指定的文件");
            }
        } catch (Exception e) {
            System.out.println("读取文件内容出错");
            e.printStackTrace();
        }
    }
}
