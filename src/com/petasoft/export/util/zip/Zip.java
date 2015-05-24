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
package com.petasoft.export.util.zip;

import java.io.File;

import org.apache.tools.ant.Project;
import org.apache.tools.ant.types.FileSet;

/**
 * @author 伍鲜 2015-05-15
 */
public class Zip {

	/**
	 * 使用zip压缩文件
	 * 
	 * @param source
	 *            需要压缩的文件
	 * @param target
	 *            压缩后文件的存放路径
	 * @param delflag
	 *            压缩后是否删除源文件
	 */
	public static void compress(File source, File target, boolean delflag) {
		if (!target.exists()) {
			target.mkdirs();
		}
		File zipFile = new File(target.getAbsolutePath() + File.separator
				+ source.getName() + ".zip");
		Project prj = new Project();
		org.apache.tools.ant.taskdefs.Zip zip = new org.apache.tools.ant.taskdefs.Zip();
		zip.setProject(prj);
		zip.setDestFile(zipFile);
		FileSet fileSet = new FileSet();
		fileSet.setProject(prj);
		fileSet.setDir(target);
		// 包括哪些文件或文件夹
		fileSet.setIncludes(source.getName());
		// 排除哪些文件或文件夹
		fileSet.setExcludes("*.zip");
		zip.addFileset(fileSet);
		zip.execute();
		if (delflag) {
			source.delete();
		}
	}
}
