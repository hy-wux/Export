/*
 * Copyright 2015 Petasoft Group.
 *  
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *  
 *      http://www.apache.org/licenses/LICENSE-2.0
 *  
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.petasoft.export.ftp;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Calendar;
import java.util.Date;

import com.petasoft.export.conf.Configuration;
import com.petasoft.export.util.DataType;
import com.petasoft.export.util.FileUtil;
import com.petasoft.export.util.PubFun;
import com.petasoft.export.util.ftp.FTP;
import com.petasoft.export.util.ftp.SFTP;

/**
 * 根据配置，传送数据到指定的服务器。
 * 
 * @author 伍鲜 2015-05-15
 */
public class FtpData extends Thread {

	private Configuration conf;

	private String logFile;

	public String intercode;
	public String incrflag;
	public String datFile;
	public String chkFile;

	private int rows = 0;

	/**
	 * intercode|srctable|incrflag
	 */
	public String key;
	public Integer time;
	/**
	 * [OK|ER]|recordRows|startTime|endTime|usedTime|datFile|chkFile
	 */
	public String status = "";

	/**
	 * 开始调用时间
	 */
	private Calendar bgnCal;
	/**
	 * 调用结束时间
	 */
	private Calendar endCal;

	public FtpData(Configuration conf, String key) {
		this.conf = conf;
		this.key = key;
		if (conf.megSucc.containsKey(key)) {
			this.time = conf.megSucc.get(key);
		} else {
			this.time = conf.expSucc.get(key);
		}
		bgnCal = Calendar.getInstance();
		endCal = Calendar.getInstance();
		start();
	}

	@Override
	public void run() {
		super.run();
		try {
			bgnCal.setTimeInMillis(System.currentTimeMillis());
			boolean ok = ftpData();
			sendResponse(ok);
		} catch (Exception e) {
			try {
				writeFile(PubFun.getExceptionString(e));
			} catch (IOException e1) {
			}
			sendResponse(false);
		}
	}

	/**
	 * 准备参数
	 * 
	 * @return
	 * @throws IOException
	 */
	private boolean parseKey() throws IOException {
		String[] procInfo = key.split(PubFun.procSplit);

		intercode = procInfo[0];

		incrflag = procInfo[2].trim();

		datFile = PubFun.getDatFileName(intercode, incrflag, time,
				conf.shortDate);
		chkFile = PubFun.getChkFileName(intercode, incrflag, time,
				conf.shortDate);
		logFile = conf.get("export.core.logsDir")
				+ "/"
				+ PubFun.getLogFileName(intercode, incrflag, time,
						conf.shortDate);

		writeFile("准备传送文件：" + key);
		writeFile("对应数据文件：" + datFile);
		writeFile("对应校验文件：" + chkFile);

		return true;
	}

	/**
	 * 循环合并文件
	 * 
	 * @return
	 * @throws Exception
	 */
	private boolean ftpData() throws Exception {
		if (parseKey()) {
			if (conf.get("export.ftp.type").equals(DataType.FTP)) {
				ftpFile(conf.get("export.core.dataDir") + "/" + datFile);

				ftpFile(conf.get("export.core.dataDir") + "/" + chkFile);
			} else if (conf.get("export.ftp.type").equals(DataType.SFTP)) {
				sftpFile(conf.get("export.core.dataDir") + "/" + datFile);

				sftpFile(conf.get("export.core.dataDir") + "/" + chkFile);
			}
			BufferedReader reader = new BufferedReader(new InputStreamReader(
					new FileInputStream(conf.get("export.core.dataDir") + "/"
							+ chkFile)));
			String line = reader.readLine();
			rows = Integer.parseInt(line.split(PubFun.tabSeparator)[1]);

			writeFile("传送文件成功");
			return true;
		}
		return false;
	}

	private void ftpFile(String filename) throws Exception {
		writeFile("FTP传送文件：" + new File(filename).getName());
		FTP.ftpFile(conf.get("export.ftp.host"), conf.get("export.ftp.port"),
				conf.get("export.ftp.user"), conf.get("export.ftp.pass"),
				conf.get("export.ftp.path"), filename,
				conf.get("export.core.CharSet"), conf.get("export.ftp.mode"));
	}

	private void sftpFile(String filename) throws Exception {
		writeFile("SFTP传送文件：" + new File(filename).getName());
		SFTP.ftpFile(conf.get("export.ftp.host"), conf.get("export.ftp.port"),
				conf.get("export.ftp.user"), conf.get("export.ftp.pass"),
				conf.get("export.ftp.path"), filename,
				conf.get("export.core.CharSet"), conf.get("export.ftp.timeout"));
	}

	private void sendResponse(boolean ok) {
		String flag;
		if (ok) {
			flag = "OK";
		} else {
			flag = "ER";
		}
		endCal.setTimeInMillis(System.currentTimeMillis());
		long diff = endCal.getTime().getTime() - bgnCal.getTime().getTime();
		status = String.format("%s|%d|%s|%s|%d|%s|%s", new Object[] { flag,
				rows, PubFun.timeFormat.format(bgnCal.getTime()),
				PubFun.timeFormat.format(endCal.getTime()), Long.valueOf(diff),
				datFile, chkFile });
	}

	/**
	 * 将日志写到文件中
	 * 
	 * @param content
	 *            日志内容
	 * @throws IOException
	 */
	private void writeFile(String content) throws IOException {
		String time = "[" + PubFun.timeFormat.format(new Date()) + "] ";
		FileUtil.appendln(logFile, time + content);
	}
}