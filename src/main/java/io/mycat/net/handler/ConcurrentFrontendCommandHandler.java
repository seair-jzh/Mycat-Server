package io.mycat.net.handler;

import org.apache.log4j.Logger;

import io.mycat.backend.mysql.MySQLMessage;
import io.mycat.config.ErrorCode;
import io.mycat.net.FrontendConnection;
import io.mycat.net.mysql.MySQLPacket;

public class ConcurrentFrontendCommandHandler extends
		AbstractFrontendCommandHandler {

	public ConcurrentFrontendCommandHandler(FrontendConnection source) {
		super(source);
	}

	private static Logger LOGGER = Logger
			.getLogger(ConcurrentFrontendCommandHandler.class);

	@Override
	public void excute(final MySQLMessage mm) {
		byte[] data = mm.bytes();
		if (source.getLoadDataInfileHandler() != null
				&& source.getLoadDataInfileHandler().isStartLoadData()) {
			int packetLength = mm.readUB3();
			if (packetLength + 4 == data.length) {
				source.loadDataInfileData(data);
			}
			return;
		}
		switch (data[4]) {
		case MySQLPacket.COM_INIT_DB:
			commands.doInitDB();
			source.initDB(data);
			break;
		case MySQLPacket.COM_QUERY:
			commands.doQuery();
			source.query(data);
			break;
		case MySQLPacket.COM_PING:
			commands.doPing();
			source.ping();
			break;
		case MySQLPacket.COM_QUIT:
			commands.doQuit();
			source.close("quit cmd");
			break;
		case MySQLPacket.COM_PROCESS_KILL:
			commands.doKill();
			source.kill(data);
			break;
		case MySQLPacket.COM_STMT_PREPARE:
			commands.doStmtPrepare();
			source.stmtPrepare(data);
			break;
		case MySQLPacket.COM_STMT_SEND_LONG_DATA:
			commands.doStmtSendLongData();
			source.stmtSendLongData(data);
			break;
		case MySQLPacket.COM_STMT_RESET:
			commands.doStmtReset();
			source.stmtReset(data);
			break;
		case MySQLPacket.COM_STMT_EXECUTE:
			commands.doStmtExecute();
			source.stmtExecute(data);
			break;
		case MySQLPacket.COM_STMT_CLOSE:
			commands.doStmtClose();
			source.stmtClose(data);
			break;
		case MySQLPacket.COM_HEARTBEAT:
			commands.doHeartbeat();
			source.heartbeat(data);
			break;
		default:
			commands.doOther();
			source.writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR,
					"Unknown command");
		}
	}

}
