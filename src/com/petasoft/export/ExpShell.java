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
package com.petasoft.export;

import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.log4j.Logger;

import com.petasoft.export.clear.DelData;
import com.petasoft.export.clear.DelLogs;
import com.petasoft.export.clear.ZipData;
import com.petasoft.export.clear.ZipLogs;
import com.petasoft.export.conf.Configuration;
import com.petasoft.export.conf.Configured;
import com.petasoft.export.conf.Tool;
import com.petasoft.export.conf.ToolRunner;
import com.petasoft.export.exp.ExpData;
import com.petasoft.export.ftp.FtpData;
import com.petasoft.export.meg.MegData;
import com.petasoft.export.proc.ExpMoni;
import com.petasoft.export.proc.ExpProc;
import com.petasoft.export.util.FileUtil;
import com.petasoft.export.util.PubFun;

/**
 * 数据卸载、合并、传输的主控进程，程序的入口。<br/>
 * 多线程实现数据的卸载、合并、传输、清理、监控。
 * 
 * @author 伍鲜 2015-05-15
 */
public class ExpShell extends Configured implements Tool {

	private static Logger logger = Logger.getLogger(ExpShell.class);

	private Map<String, Integer> megTime = new HashMap<String, Integer>();
	private Map<String, Integer> ftpTime = new HashMap<String, Integer>();

	@Override
	public int run(String[] args) throws Exception {
		int exitCode = -1;
		if (args.length < 1) {
			printUsage();
			System.out.println("Please with TranDate parameter.");
			System.out.println("You can use Export like this:");
			System.out.println("   Export "
					+ PubFun.dateHFormat.format(new Date()));
			System.out.println("OR ");
			System.out.println("   Export "
					+ PubFun.shortFormat.format(new Date()));
			return exitCode;
		}
		if (args.length >= 1) {
			if (args[0].trim().length() == 8) {
				getConf().tranDate = PubFun.dateHFormat
						.format(PubFun.shortFormat.parse(args[0]));
			} else if (args[0].trim().length() == 10) {
				if (args[0].trim().contains("-")) {
					getConf().tranDate = PubFun.dateHFormat
							.format(PubFun.dateHFormat.parse(args[0]));
				} else if (args[0].trim().contains("/")) {
					getConf().tranDate = PubFun.dateHFormat
							.format(PubFun.dateXFormat.parse(args[0]));
				} else {
					throw new Exception("Unparseable date: \"" + args[0] + "\"");
				}
			} else {
				throw new Exception("Unparseable date: \"" + args[0] + "\"");
			}
			getConf().shortDate = PubFun.shortFormat.format(PubFun.dateHFormat
					.parse(getConf().tranDate));
		}
		getConf().load();

		// 删除
		new DelData(getConf()).start();
		new DelLogs(getConf()).start();

		// 压缩
		new ZipData(getConf()).start();
		new ZipLogs(getConf()).start();

		// 监控
		new ExpMoni(getConf()).start();

		getConf().ftp = ftpFlag();
		while (true) {
			startExp();
			checkExp();

			startMeg();
			checkMeg();

			if (getConf().ftp) {
				startFtp();
				checkFtp();
			}

			if (getConf().expMap.isEmpty() && getConf().megMap.isEmpty()
					&& getConf().ftpMap.isEmpty()
					&& getConf().expList.size() == 0
					&& getConf().megList.size() == 0
					&& getConf().ftpList.size() == 0) {
				break;
			}
			Thread.sleep(Integer.parseInt(getConf().get(
					"export.core.ThreadSleep")) * 1000);
		}
		if (getConf().ftp) {
			logger.info("prepare ftp export proc log.");
			new ExpProc(getConf()).ftpProcLog();
			logger.info("success ftp export proc log.");
		}
		logger.info("Petasoft Export v1.5 Copyright 2015. All thread finished successfull. Thank you!");
		Thread.sleep(5000);
		return 0;
	}

	private boolean ftpFlag() {
		if ("".equals(getConf().get("export.ftp.host"))) {
			return false;
		}
		if ("".equals(getConf().get("export.ftp.user"))) {
			return false;
		}
		return true;
	}

	/**
	 * 判断是否可以直接FTP<br/>
	 * 如果存在合并，就不直接FTP，而是等合并完成后再FTP
	 * 
	 * @param key
	 *            intercode|srctable|incrflag
	 * @return
	 */
	private boolean ftpFlag(String key) {
		if (getConf().megMap.containsKey(key.split(PubFun.procSplit)[0])) {
			return false;
		}
		for (String intercode : getConf().megMap.keySet()) {
			if (getConf().megMap
					.get(intercode)
					.concat(PubFun.comSeparator)
					.contains(
							key.split(PubFun.procSplit)[0]
									.concat(PubFun.comSeparator))) {
				return false;
			}
		}
		return true;
	}

	/**
	 * 执行导出数据
	 */
	private void startExp() {
		Iterator<String> keys = getConf().expMap.keySet().iterator();
		while (keys.hasNext()) {
			String key = keys.next();
			// 控制卸载线程数
			if (getConf().expCount < Integer.parseInt(getConf().get(
					"export.core.ThreadExport"))) {
				logger.info("start export file. key=" + key);
				getConf().expList.add(new ExpData(getConf(), key));
				getConf().expCount++;
			}
		}

		Iterator<ExpData> list = getConf().expList.iterator();
		while (list.hasNext()) {
			getConf().expMap.remove(list.next().key);
		}
	}

	/**
	 * 检查导出状态
	 * 
	 * @throws Exception
	 */
	private void checkExp() throws Exception {
		for (int i = getConf().expList.size() - 1; i > -1; i--) {
			ExpData expData = getConf().expList.get(i);
			String status = expData.status;
			if (!"".equals(status)) {
				String flag = status.split(PubFun.procSplit)[0];
				if ("OK".equals(flag)) {
					// 导出成功
					logger.info("success export file. key=" + expData.key);
					getConf().expSucc.put(expData.key, expData.time);
					FileUtil.appendln(
							getConf().EXP_STATUS_INI,
							"EXP"
									+ PubFun.procConcat
									+ expData.key.substring(0, expData.key
											.lastIndexOf(PubFun.procConcat))
									+ PubFun.procConcat + expData.status
									+ PubFun.procConcat + getConf().shortDate);
					if (getConf().ftp && ftpFlag(expData.key)) {
						getConf().ftpMap.put(expData.key, expData.time);
					}
				} else {
					// 导出失败
					logger.info("failure export file. key=" + expData.key);
					// 每个任务最多导出5次，连续5次导出不成功即为失败
					if (expData.time < 5) {
						getConf().expMap.put(expData.key, expData.time + 1);
					} else {
						FileUtil.appendln(
								getConf().EXP_STATUS_ERR,
								"EXP"
										+ PubFun.procConcat
										+ expData.key.substring(0, expData.key
												.lastIndexOf(PubFun.procConcat))
										+ PubFun.procConcat + expData.status
										+ PubFun.procConcat
										+ getConf().shortDate);
					}
				}
				getConf().expList.remove(i);
				getConf().expCount--;
			}
		}
	}

	/**
	 * 判断是否可以执行合并
	 * 
	 * @param intercode
	 * @return
	 */
	private boolean canMEG(String intercode) {
		boolean k = false;
		boolean v = true;
		if (getConf().interMap.containsKey(intercode)
				&& !getConf().expSucc.containsKey(intercode.concat(
						PubFun.procConcat).concat(
						getConf().interMap.get(intercode)))) {
			// 接口需要导出，但是导出并未完成
			k = false;
		}

		if (!getConf().interMap.containsKey(intercode)
				|| getConf().expSucc.containsKey(intercode.concat(
						PubFun.procConcat).concat(
						getConf().interMap.get(intercode)))) {
			// 接口不需要导出，或者导出完成
			k = true;
		}

		String[] subs = getConf().megMap.get(intercode).split(",");
		for (int i = 0; i < subs.length; i++) {
			String sub = subs[i];
			if (!getConf().interMap.containsKey(sub)) {
				// 不存在的接口
			} else if (getConf().expSucc.containsKey(sub.concat(
					PubFun.procConcat).concat(getConf().interMap.get(sub)))) {
				// 接口存在，并且已经导出完成
			} else {
				// 接口存在，但是并未导出完成
				v = false;
				break;
			}
		}
		return k && v;
	}

	/**
	 * 执行文件合并
	 */
	private void startMeg() {
		Iterator<String> intercodes = getConf().megMap.keySet().iterator();
		while (intercodes.hasNext()) {
			String intercode = intercodes.next();
			if (canMEG(intercode)) {
				logger.info("start merge file. intercode=" + intercode);
				getConf().megList.add(new MegData(getConf(), intercode));
				if (megTime.containsKey(intercode)) {
					megTime.put(intercode, megTime.get(intercode) + 1);
				} else {
					megTime.put(intercode, 1);
				}
			}
		}

		Iterator<MegData> list = getConf().megList.iterator();
		while (list.hasNext()) {
			getConf().megMap.remove(list.next().intercode);
		}
	}

	/**
	 * 检查合并状态
	 */
	private void checkMeg() throws Exception {
		for (int i = getConf().megList.size() - 1; i > -1; i--) {
			MegData megData = getConf().megList.get(i);
			String status = megData.status;
			if (!"".equals(status)) {
				String flag = status.split(PubFun.procSplit)[0];
				if ("OK".equals(flag)) {
					// 合并成功
					logger.info("success merge file. key=" + megData.key);
					getConf().megSucc.put(megData.key, megData.time);
					if (getConf().ftp) {
						getConf().ftpMap.put(megData.key, megData.time);
					}
					FileUtil.appendln(
							getConf().MEG_STATUS_INI,
							"MEG"
									+ PubFun.procConcat
									+ megData.key.substring(0, megData.key
											.lastIndexOf(PubFun.procConcat))
									+ PubFun.procConcat + megData.status
									+ PubFun.procConcat + getConf().shortDate);
				} else {
					// 合并失败
					logger.info("failure merge file. key=" + megData.key);
					// 每个任务最多合并5次，连续5次合并不成功即为失败
					if (megTime.get(megData.intercode) < 5) {
						getConf().megMap.put(megData.intercode,
								megData.subcodes);
					} else {
						FileUtil.appendln(
								getConf().MEG_STATUS_ERR,
								"MEG"
										+ PubFun.procConcat
										+ megData.key.substring(0, megData.key
												.lastIndexOf(PubFun.procConcat))
										+ PubFun.procConcat + megData.status
										+ PubFun.procConcat
										+ getConf().shortDate);
					}
				}
				getConf().megList.remove(i);
			}
		}
	}

	/**
	 * 执行传送数据
	 */
	private void startFtp() {
		Iterator<String> keys = getConf().ftpMap.keySet().iterator();
		while (keys.hasNext()) {
			String key = keys.next();
			logger.info("start ftp file. key=" + key);
			getConf().ftpList.add(new FtpData(getConf(), key));
			if (ftpTime.containsKey(key)) {
				ftpTime.put(key, ftpTime.get(key) + 1);
			} else {
				ftpTime.put(key, 1);
			}
		}

		Iterator<FtpData> list = getConf().ftpList.iterator();
		while (list.hasNext()) {
			getConf().ftpMap.remove(list.next().key);
		}
	}

	/**
	 * 检查传送状态
	 */
	private void checkFtp() throws Exception {
		for (int i = getConf().ftpList.size() - 1; i > -1; i--) {
			FtpData ftpData = getConf().ftpList.get(i);
			String status = ftpData.status;
			if (!"".equals(status)) {
				String flag = status.split(PubFun.procSplit)[0];
				if ("OK".equals(flag)) {
					// 传送成功
					logger.info("success ftp file. key=" + ftpData.key);
					getConf().ftpSucc.put(ftpData.key, ftpData.time);
					FileUtil.appendln(
							getConf().FTP_STATUS_INI,
							"FTP"
									+ PubFun.procConcat
									+ ftpData.key.substring(0, ftpData.key
											.lastIndexOf(PubFun.procConcat))
									+ PubFun.procConcat + ftpData.status
									+ PubFun.procConcat + getConf().shortDate);
				} else {
					// 传送失败
					logger.info("failure ftp file. key=" + ftpData.key);
					// 每个任务最多传送5次，连续5次传送不成功即为失败
					if (megTime.get(ftpData.key) < 5) {
						getConf().ftpMap.put(ftpData.key, ftpData.time);
					} else {
						FileUtil.appendln(
								getConf().FTP_STATUS_ERR,
								"FTP"
										+ PubFun.procConcat
										+ ftpData.key.substring(0, ftpData.key
												.lastIndexOf(PubFun.procConcat))
										+ PubFun.procConcat + ftpData.status
										+ PubFun.procConcat
										+ getConf().shortDate);
					}
				}
				getConf().ftpList.remove(i);
			}
		}
	}

	/**
	 * Displays format of commands.
	 * 
	 */
	private static void printUsage() {
		ToolRunner.printGenericCommandUsage();
	}

	public static void main(String[] args) {
		args = new String[] { "2015/05/01" };
		ExpShell shell = new ExpShell();
		shell.setConf(new Configuration());
		int res = 0;
		try {
			res = ToolRunner.run(shell, args);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
		}
		System.exit(res);
	}
}
