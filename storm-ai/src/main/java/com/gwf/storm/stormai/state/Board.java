package com.gwf.storm.stormai.state;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * 棋盘状态
 * @author gaowenfeng
 */
@Slf4j
@Data
public class Board implements Serializable{
    private static final int NUMBER_OF_BOARD = 3;
    private static final int VICTORY_SIGN = 100;
    private static final int THREE_IS_THE_SAME_SCORE = 10000;
    private static final int TWO_IS_THE_SAME_SCORE = 100;
    private static final int ONE_IS_THE_SAME_SCORE = 10;
    private static final String EMPTY = " ";

    private String[][] board = {{EMPTY,EMPTY,EMPTY},{EMPTY,EMPTY,EMPTY}};

    /**
     * 子节点棋盘状态
     * @param player
     * @return
     */
    public List<Board> nextBoards(String player) {
        List<Board> boards = new ArrayList<>();
        for(int i=0;i<NUMBER_OF_BOARD;i++){
            for(int j=0;j<NUMBER_OF_BOARD;j++) {
                if(EMPTY.equals(this.board[i][j])){
                    try {
                        Board newBoard = (Board) this.clone();
                        newBoard.board[i][j] = player;
                        boards.add(newBoard);
                    } catch (CloneNotSupportedException e) {
                        log.error("克隆Board异常",e);
                    }
                }
            }
        }
        return boards;
    }

    /**
     * 判断是否为结束状态
     * 当棋盘旗子满了 或者 分数大于100的时候说明为结束状态
     * @return
     */
    public boolean isEndState(){
        return (0 == nextBoards(Player.PLAYER_X).size() || Math.abs(score(Player.PLAYER_X))>VICTORY_SIGN);
    }

    /**
     * 计算唯一键
     * @return
     */
    public String toKey(){
        StringBuilder sb = new StringBuilder();
        for(int i=0;i<3;i++){
            for(int j=0;j<3;i++){
                sb.append(board[i][j]);
            }
        }
        return sb.toString();
    }

    /**
     * 计算分数
     * @param player
     * @return
     */
    private int score(String player) {
        return scoreLines(player)-scoreLines(Player.next(player));
    }



    private int scoreLines(String player) {
        int score = 0;
        //Columns
        score += scoreLine(this.board[0][0],this.board[1][0],this.board[2][0],player);
        score += scoreLine(this.board[0][1],this.board[1][1],this.board[2][1],player);
        score += scoreLine(this.board[0][2],this.board[1][2],this.board[2][2],player);

        //Rows
        score += scoreLine(this.board[0][0],this.board[0][1],this.board[0][2],player);
        score += scoreLine(this.board[1][0],this.board[1][1],this.board[1][2],player);
        score += scoreLine(this.board[2][0],this.board[2][1],this.board[2][2],player);

        //Diagonals
        score += scoreLine(this.board[0][0],this.board[1][1],this.board[2][2],player);
        score += scoreLine(this.board[2][0],this.board[1][1],this.board[0][2],player);
        return score;
    }

    private int scoreLine(String pos1, String pos2, String pos3, String player) {
        int score = 0;

        boolean _XX = (pos1.equals(EMPTY) && pos2.equals(player) && pos3.equals(player));
        boolean X_X = (pos1.equals(player) && pos2.equals(EMPTY) && pos3.equals(player));
        boolean XX_ = (pos1.equals(player) && pos2.equals(player) && pos3.equals(EMPTY));

        if(pos1.equals(player) && pos2.equals(player) && pos3.equals(player)){
            score = THREE_IS_THE_SAME_SCORE;
        }else if(XX_ || _XX || X_X){
            score = TWO_IS_THE_SAME_SCORE;
        }else {
            boolean X__ = (pos1.equals(player) && pos2.equals(EMPTY) && pos3.equals(EMPTY));
            boolean _X_ = (pos1.equals(EMPTY) && pos2.equals(player) && pos3.equals(EMPTY));
            boolean __X = (pos1.equals(EMPTY) && pos2.equals(EMPTY) && pos3.equals(player));
            if(X__||_X_||__X){
                score = ONE_IS_THE_SAME_SCORE;
            }
        }
        return score;
    }


}
