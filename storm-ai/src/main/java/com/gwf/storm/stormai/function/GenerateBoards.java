package com.gwf.storm.stormai.function;

import com.gwf.storm.stormai.state.Board;
import com.gwf.storm.stormai.state.GameState;
import com.gwf.storm.stormai.state.Player;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.List;

/**
 * @author gaowenfeng
 */
@Slf4j
@Service
public class GenerateBoards extends BaseFunction{

    @Override
    public void execute(TridentTuple tuple, TridentCollector tridentCollector) {
        GameState gameState = (GameState) tuple.get(0);
        Board currentBoard = gameState.getBoard();
        List<Board> history = new ArrayList<>();
        history.addAll(gameState.getHistory());
        history.add(currentBoard);

        if(!currentBoard.isEndState()){
            String nextPlayer = Player.next(gameState.getPlayer());
            List<Board> boards = gameState.getBoard().nextBoards(nextPlayer);
            log.debug("Generated [{}] children boards for [{}]",boards.size(),gameState.toString());

            for(Board b:boards){
                GameState newGameState = new GameState(b,history,nextPlayer);
                List<Object> values = new ArrayList<>();
                values.add(newGameState);
                tridentCollector.emit(values);
            }
        }else {
            log.debug("End game fund! [{}]",currentBoard);
        }
    }
}
