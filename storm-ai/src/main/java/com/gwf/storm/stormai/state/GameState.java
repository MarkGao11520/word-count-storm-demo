package com.gwf.storm.stormai.state;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;
import java.util.List;

/**
 * 游戏状态
 * @author gaowenfeng
 */
@Data
@AllArgsConstructor
public class GameState implements Serializable{
    private Board board;
    private List<Board> history;
    private String player;

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(board.toKey()).append(']')
                .append(": player(").append(player).append(")\n")
                .append(" history [");
        for(Board b:history){
            sb.append(b.toKey()).append(',');
        }
        sb.append(']');
        return sb.toString();
    }
}
