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
package com.petasoft.export.meg;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Calendar;
import java.util.Date;

import com.petasoft.export.conf.Configuration;
import com.petasoft.export.util.FileUtil;
import com.petasoft.export.util.PubFun;

/**
 * 根据配置，实现数据文件的合并。
 * 
 * @author 伍鲜 2015-05-15
 */
public class MegData extends Thread {
	private Configuration conf;

	private String logFile;

	private int rows = 0;
	private long size;

	public String intercode;
	public String incrflag;
	public String datFile;
	public String chkFile;

	/**
	 * intercode|srctable|incrflag
	 */
	public String key;
	public String subcodes;
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

	public MegData(Configuration conf, String intercode) {
		this.conf = conf;
		this.intercode = intercode;
		this.subcodes = conf.megMap.get(intercode);
		if (intercode.equals(conf.get("export.core.QualityCode"))) {
			this.key = String.format(
					"%s|%s|%s",
					new Object[] { intercode,
							conf.get("export.core.QualityCode"), "z" });
			this.incrflag = "z";
			this.time = 1;
		} else if (conf.interMap.containsKey(intercode)) {
			this.key = String.format("%s|%s", new Object[] { intercode,
					conf.interMap.get(intercode) });
			this.incrflag = key.split(PubFun.procSplit)[2];
			this.time = conf.expSucc.get(key);
		} else {
			this.key = String.format("%s|%s|%s", new Object[] { intercode,
					intercode, "z" });
			this.incrflag = key.split(PubFun.procSplit)[2];
			this.time = 1;
		}

		datFile = PubFun.getDatFileName(intercode, incrflag, time,
				conf.shortDate);
		chkFile = PubFun.getChkFileName(intercode, incrflag, time,
				conf.shortDate);
		logFile = conf.get("export.core.logsDir")
				+ "/"
				+ PubFun.getLogFileName(intercode, incrflag, time,
						conf.shortDate);

		// 如果是合成后的接口，删除原有文件
		if (!conf.interMap.containsKey(intercode)) {
			File file = null;
			file = new File(conf.get("export.core.dataDir") + "/" + datFile);
			if (file.exists()) {
				file.delete();
			}
			file = new File(conf.get("export.core.dataDir") + "/" + chkFile);
			if (file.exists()) {
				file.delete();
			}
			file = new File(logFile);
			if (file.exists()) {
				file.delete();
			}
		} else {
			BufferedReader reader = null;
			String line = null;
			try {
				reader = new BufferedReader(new InputStreamReader(
						new FileInputStream(conf.get("export.core.dataDir")
								+ "/" + chkFile)));
				line = reader.readLine();
				rows += Integer.parseInt(line.split(PubFun.tabSeparator)[1]);
				size += Integer.parseInt(line.split(PubFun.tabSeparator)[2]);
			} catch (FileNotFoundException e) {
			} catch (IOException e) {
			}
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
			boolean ok = merge();
			sendResponse(ok);
		} catch (Exception e) {
			sendResponse(false);
			try {
				writeFile(PubFun.getExceptionString(e));
			} catch (IOException e1) {
			}
		}
	}

	private boolean merge() throws Exception {
		writeFile("准备合并数据：" + key);
		writeFile("合并数据文件：" + datFile);
		writeFile("合并校验文件：" + chkFile);

		String[] subs = subcodes.split(PubFun.comSeparator);

		BufferedReader reader = null;
		String line = null;

		for (int i = 0; i < subs.length; i++) {
			String subCode = subs[i];
			if (subCode.equals(intercode)) {
				// 自身合并到自身的跳过
				continue;
			}
			if (!conf.interMap.containsKey(subCode)) {
				// 接口不存在的合并忽略
				continue;
			}
			String subKey = subCode.concat(PubFun.procConcat).concat(
					conf.interMap.get(subCode));
			String subFlag = subKey.split(PubFun.procSplit)[2];
			int subTime = conf.expSucc.get(subKey);

			String datSub = PubFun.getDatFileName(subCode, subFlag, subTime,
					conf.shortDate);
			String chkSub = PubFun.getChkFileName(subCode, subFlag, subTime,
					conf.shortDate);

			writeFile("合并文件[" + datSub + "]到[" + datFile + "]");

			reader = new BufferedReader(new InputStreamReader(
					new FileInputStream(conf.get("export.core.dataDir") + "/"
							+ datSub)));
			while ((line = reader.readLine()) != null) {
				if (!"".equals(line)) {
					FileUtil.appendln(conf.get("export.core.dataDir") + "/"
							+ datFile, line);
				}
			}
			reader = new BufferedReader(new InputStreamReader(
					new FileInputStream(conf.get("export.core.dataDir") + "/"
							+ chkSub)));
			line = reader.readLine();
			rows += Integer.parseInt(line.split(PubFun.tabSeparator)[1]);
			size += Integer.parseInt(line.split(PubFun.tabSeparator)[2]);
			FileUtil.overrid(
					conf.get("export.core.dataDir") + "/" + chkFile,
					String.format("%s\t%d\t%d\t%s",
							new Object[] { this.datFile, rows, size,
									PubFun.timeFormat.format(new Date()) }));
			writeFile("成功合并文件[" + datSub + "]到[" + datFile + "] 记录数：" + rows);
		}

		return true;
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
