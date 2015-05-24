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
package com.petasoft.export.proc;

import com.petasoft.export.conf.Configuration;

/**
 * 主控进程的监控。
 * 
 * @author 伍鲜 2015-05-15
 */
public class ExpMoni extends Thread {
	private Configuration conf;

	public ExpMoni(Configuration conf) {
		this.conf = conf;
	}

	@Override
	public void run() {
		super.run();
		while (true) {
			System.out.println("Total export: " + conf.interMap.size() + ".");
			System.out.println("Total merge: " + conf.mergeMap.size() + ".");
			System.out.println("Total ftp: " + conf.ftpInter.size() + ".");
			System.out.println("Waiting export count: " + conf.expMap.size()
					+ ". Current export count: " + conf.expList.size()
					+ ". Success export count: " + conf.expSucc.size());
			System.out.println("Waiting merge count: " + conf.megMap.size()
					+ ". Current merge count: " + conf.megList.size()
					+ ". Success merge count: " + conf.megSucc.size());
			System.out.println("Waiting ftp count: " + conf.ftpMap.size()
					+ ". Current ftp count: " + conf.ftpList.size()
					+ ". Success ftp count: " + conf.ftpSucc.size());
			System.out.println();
			try {
				Thread.sleep(2000);
			} catch (InterruptedException e) {
			}
		}
	}
}
