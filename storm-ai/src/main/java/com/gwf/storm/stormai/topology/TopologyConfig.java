package com.gwf.storm.stormai.topology;

import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import com.gwf.storm.stormai.filter.IsEndGame;
import com.gwf.storm.stormai.function.GenerateBoards;
import com.gwf.storm.stormai.function.LocalQueueFunction;
import com.gwf.storm.stormai.spout.LocalQueueEmitter;
import com.gwf.storm.stormai.spout.DefaultCoordinator;
import com.gwf.storm.stormai.spout.LocalQueueSpout;
import com.gwf.storm.stormai.state.Board;
import com.gwf.storm.stormai.state.GameState;
import com.gwf.storm.stormai.state.Player;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import storm.trident.Stream;
import storm.trident.TridentTopology;

import java.util.ArrayList;

/**
 * @author gaowenfeng
 */
@Configuration
@Slf4j
public class TopologyConfig {

    @Autowired
    private IsEndGame isEndGame;

    @Autowired
    private GenerateBoards generateBoards;

    @Bean
    public LocalQueueEmitter<GameState> workSpoutEmitter(){
        return new LocalQueueEmitter<GameState>("WorkQueue");
    }

    @Bean
    public LocalQueueEmitter<GameState> scoringSpoutEmitter(){
        return new LocalQueueEmitter<GameState>("ScoringQueue");
    }

    @Bean
    public LocalQueueSpout<GameState> workSpout(){
        return new LocalQueueSpout<GameState>(new DefaultCoordinator(),workSpoutEmitter());
    }

    @Bean
    public LocalQueueFunction workQueueFunction(){
        return new LocalQueueFunction(workSpoutEmitter());
    }

    @Bean
    public LocalQueueFunction scoreQueueFunction(){
        return new LocalQueueFunction(scoringSpoutEmitter());
    }


    @Bean
    public StormTopology RecursiveTopology(){
        log.debug("Building topology.");
        TridentTopology topology = new TridentTopology();

        //Work Queue /Spout
        GameState initialState = new GameState(new Board(),new ArrayList<Board>(), Player.PLAYER_X);
        workSpoutEmitter().enqueue(initialState);

        //Scoring Queue /Spout

        Stream inputStream = topology.newStream("gamestate",workSpout());

        inputStream
                .each(new Fields("gamestate"),isEndGame)
                .each(new Fields("gamestate"),workQueueFunction(),new Fields(""));
        inputStream
                .each(new Fields("gamestate"),generateBoards,new Fields("children"))
                .each(new Fields("children"),scoreQueueFunction(),new Fields());
        return topology.build();
    }
}
