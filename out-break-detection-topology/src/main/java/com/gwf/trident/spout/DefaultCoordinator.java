package com.gwf.trident.spout;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.spout.ITridentSpout;

import java.io.Serializable;

/**
 * BatchCoordinator 是一个泛型类，重放一个batch所需要的元数据
 * 实际系统中，元数据可能包含组成了这个batch的消息或者对象的标识符。
 * 通过这个信息，非透明型和事务型spout可以实现约定
 * BatchCoordinator 作为一个storm bolt运行在一个单线程中。
 * Storm会在Zookeeper中持久化存储这个元数据，当事务处理完成时会通知对应的coordinator
 */
public class DefaultCoordinator implements ITridentSpout.BatchCoordinator<Long>,Serializable{
    private static final long serialVersionUID = 3067582229400330097L;
    private static final Logger LOG = LoggerFactory.getLogger(DefaultCoordinator.class);

    @Override
    public Long initializeTransaction(long txid, Long prevMetadata, Long x1) {
        LOG.info("Initializing Transaction ["+txid+"],["+x1+"]");
        return null;
    }

    @Override
    public void success(long txid) {
        LOG.info("Successful Transaction ["+txid+"]");
    }

    @Override
    public boolean isReady(long txid) {
        return true;
    }

    @Override
    public void close() {

    }
}
