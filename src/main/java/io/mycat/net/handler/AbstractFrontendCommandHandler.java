package io.mycat.net.handler;

import io.mycat.backend.mysql.MySQLMessage;
import io.mycat.net.FrontendConnection;
import io.mycat.net.NIOHandler;
import io.mycat.statistic.CommandCount;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

//create by jiezhia to support concurrent request 
public abstract class AbstractFrontendCommandHandler implements NIOHandler{
	private static Logger LOGGER = Logger.getLogger(AbstractFrontendCommandHandler.class);
	
	protected final FrontendConnection source;
    protected final CommandCount commands;

    /**
     * rowData缓存队列
     */
    protected BlockingQueue<MySQLMessage> packs = new LinkedBlockingQueue<MySQLMessage>();

    /**
     * 标志业务线程是否启动了？
     */
    protected final AtomicBoolean running = new AtomicBoolean(false);

    public AbstractFrontendCommandHandler(FrontendConnection source){
    	this.source = source;
        this.commands = source.getProcessor().getCommands();
    }

    /**
     * Add a row pack, and may be wake up a business thread to work if not running.
     * @param pack row pack
     * @return true wake up a business thread, otherwise false
     *
     * @author Uncle-pan
     * @since 2016-03-23
     */
    protected final boolean addPack(final MySQLMessage mm){
        packs.add(mm);
        if(!running.get()){
        	this.cycleWork();
        }
        return true;
    }


    @Override
    public void handle(byte[] data)
    {
    	MySQLMessage mm = new MySQLMessage(data);
        addPack(mm);
    }
    
    public void cycleWork()
    {
    	if (!running.compareAndSet(false, true)) {
			return;
		}
    	
    	try{
    		final MySQLMessage mm = packs.poll();

			if (mm != null) {
				excute(mm);
			}
    	}finally{
    		running.set(false);
			if (!packs.isEmpty()) {
				this.cycleWork();
			}
    	}
    }

    /**
     * 執行處理邏輯
     * @return (最多i*(offset+size)行数据)
     */
    public abstract void excute(final MySQLMessage mm);
}
