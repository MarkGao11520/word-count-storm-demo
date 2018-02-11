package com.gwf.storm.stormai.filter;

import com.gwf.storm.stormai.state.GameState;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;

/**
 * @author gaowenfeng
 */
@Slf4j
@Service
public class IsEndGame extends BaseFilter{
    @Override
    public boolean isKeep(TridentTuple tuple) {
        GameState gameState = (GameState) tuple.get(0);
        boolean keep = gameState.getBoard().isEndState();
        if(keep){
            log.debug("END GAME [{}]",gameState);
        }
        return keep;
    }
}
