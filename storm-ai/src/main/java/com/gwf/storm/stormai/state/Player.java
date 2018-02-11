package com.gwf.storm.stormai.state;

/**
 * 玩家
 * @author gaowenfeng
 */
public class Player {
    public static final String PLAYER_X = "X";
    public static final String PLAYER_O = "0";

    public static String next(String current){
        if(current.equals(PLAYER_X)){
            return PLAYER_O;
        }
        return PLAYER_X;
    }
}
