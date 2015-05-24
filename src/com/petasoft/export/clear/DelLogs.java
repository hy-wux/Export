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
package com.petasoft.export.clear;

import java.io.File;
import java.util.Calendar;

import com.petasoft.export.conf.Configuration;
import com.petasoft.export.util.PubFun;


/**
 * 日志文件删除
 * 
 * @author 伍鲜 2015-05-15
 */
public class DelLogs extends Thread {

	private Configuration conf;

	private int delDay;

	/**
	 * YYYY-MM-DD
	 */
	private String tranDate;

	/**
	 * YYYY-MM-DD
	 */
	private String dropDate;

	public DelLogs(Configuration conf) {
		this.conf = conf;
		this.tranDate = conf.tranDate;
		this.delDay = Integer.parseInt(conf.get("export.core.DelLogs"));
	}

	@Override
	public void run() {
		super.run();
		prepareDate();
		String directory = PubFun.replaceDate(
				conf.get("export.core.logsDir.String"), dropDate);
		File parent = new File(directory);
		if (parent.exists()) {
			File[] files = parent.listFiles();
			for (File file : files) {
				if (file.getName().substring(0, 8).equals(dropDate)) {
					// 仅删除日志文件
					if (file.getName().endsWith(".log")
							|| file.getName().endsWith(".log.zip")) {
						deleteFile(file);
					}
				}
			}
		}
	}

	/**
	 * 计算日期
	 */
	private void prepareDate() {
		String[] tmpStr = tranDate.split("-");
		Calendar curCal = Calendar.getInstance();
		curCal.set(Integer.parseInt(tmpStr[0]),
				Integer.parseInt(tmpStr[1]) - 1, Integer.parseInt(tmpStr[2]));
		Calendar cal = Calendar.getInstance();

		cal.setTime(curCal.getTime());
		cal.add(Calendar.DATE, 0 - delDay);
		dropDate = PubFun.shortFormat.format(cal.getTime());
	}

	private void deleteFile(File parent) {
		if (parent.isDirectory()) {
			File[] files = parent.listFiles();
			for (File file : files) {
				deleteFile(file);
			}
		}
		parent.delete();
	}

	public static void main(String[] args) {
	}
}
