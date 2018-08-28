package com.gwf.storm;


import java.util.HashMap;

/**
 * @author gaowenfeng
 */
public class Application {

    private static final HashMap<Integer,String> map = new HashMap<>();
    public static void main(String[] args) throws InterruptedException {
        int insertNum = 1000000;
        Thread t1 = new Thread(() -> {
            for(int i=0;i<insertNum;i++){
                map.put(i,null);
            }
        });

        Thread t2 = new Thread(() -> {
            for(int i=insertNum;i<2*insertNum;i++){
                map.put(i,null);
            }
        });

        t1.start();
        t2.start();

        t1.join();
        t2.join();


        System.out.println(map.size());
    }
}
