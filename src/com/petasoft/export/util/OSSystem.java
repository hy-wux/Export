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
package com.petasoft.export.util;

/**
 * @author ÎéÏÊ 2015-05-15
 */
public final class OSSystem {

	private static String system = System.getProperty("os.name").toLowerCase();

	private OSSystem() {
	}

	public static boolean isLinux() {
		return system.indexOf("linux") >= 0;
	}

	public static boolean isMacsystem() {
		return system.indexOf("mac") >= 0 && system.indexOf("os") > 0
				&& system.indexOf("x") < 0;
	}

	public static boolean isWindows() {
		return system.indexOf("windows") >= 0;
	}

	public static boolean issystem2() {
		return system.indexOf("os/2") >= 0;
	}

	public static boolean isSolaris() {
		return system.indexOf("solaris") >= 0;
	}

	public static boolean isSunsystem() {
		return system.indexOf("sunos") >= 0;
	}

	public static boolean isMPEiX() {
		return system.indexOf("mpe/ix") >= 0;
	}

	public static boolean isHPUX() {
		return system.indexOf("hp-ux") >= 0;
	}

	public static boolean isAix() {
		return system.indexOf("aix") >= 0;
	}

	public static boolean issystem390() {
		return system.indexOf("os/390") >= 0;
	}

	public static boolean isFreeBSD() {
		return system.indexOf("freebsd") >= 0;
	}

	public static boolean isIrix() {
		return system.indexOf("irix") >= 0;
	}

	public static boolean isDigitalUnix() {
		return system.indexOf("digital") >= 0 && system.indexOf("unix") > 0;
	}

	public static boolean isNetWare() {
		return system.indexOf("netware") >= 0;
	}

	public static boolean issystemF1() {
		return system.indexOf("osf1") >= 0;
	}

	public static boolean isOpenVMS() {
		return system.indexOf("openvms") >= 0;
	}

	public static String getsystemName() {
		return system;
	}
}
