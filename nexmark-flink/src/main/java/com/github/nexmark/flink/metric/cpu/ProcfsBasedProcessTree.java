/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.nexmark.flink.metric.cpu;

import com.github.nexmark.flink.metric.cpu.clock.Clock;
import com.github.nexmark.flink.metric.cpu.clock.SystemClock;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.filefilter.AndFileFilter;
import org.apache.commons.io.filefilter.DirectoryFileFilter;
import org.apache.commons.io.filefilter.RegexFileFilter;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A Proc file-system based ProcessTree. Works only on Linux.
 * Based on ProcfsBasedProcessTree from YARN project.
 */
public class ProcfsBasedProcessTree {
	private static final Logger LOG = LoggerFactory.getLogger(ProcfsBasedProcessTree.class);

	private static final String PROCFS = "/proc/";

	private static final String SELF = "self";

	private static final Pattern PROCFS_STAT_FILE_FORMAT = Pattern.compile(
		"^([\\d-]+)\\s\\((.*)\\)\\s[^\\s]\\s([\\d-]+)\\s([\\d-]+)\\s" +
			"([\\d-]+)\\s([\\d-]+\\s){7}(\\d+)\\s(\\d+)\\s([\\d-]+\\s){7}(\\d+)\\s" +
			"(\\d+)(\\s[\\d-]+){15}");

	public static final int UNAVAILABLE = -1;
	public static final String PROCFS_STAT_FILE = "stat";
	public static final String PROCFS_CMDLINE_FILE = "cmdline";
	public static final long PAGE_SIZE = SysInfoLinux.PAGE_SIZE;
	public static final long JIFFY_LENGTH_IN_MILLIS =
		SysInfoLinux.JIFFY_LENGTH_IN_MILLIS; // in millisecond
	private final CpuTimeTracker cpuTimeTracker;
	private final CpuTimeTracker taskCpuTracker;
	private final CpuTimeTracker forstCoorTracker;
	private final CpuTimeTracker forstWriteTracker;
	private final CpuTimeTracker forstReadTracker;
	private final CpuTimeTracker rocksLowTracker;
	private final CpuTimeTracker rocksHighTracker;

	private Clock clock;

	enum MemInfo {
		SIZE("Size"), RSS("Rss"), PSS("Pss"), SHARED_CLEAN("Shared_Clean"),
		SHARED_DIRTY("Shared_Dirty"), PRIVATE_CLEAN("Private_Clean"),
		PRIVATE_DIRTY("Private_Dirty"), REFERENCED("Referenced"), ANONYMOUS(
			"Anonymous"), ANON_HUGE_PAGES("AnonHugePages"), SWAP("swap"),
		KERNEL_PAGE_SIZE("kernelPageSize"), MMU_PAGE_SIZE("mmuPageSize"), INVALID(
			"invalid");

		private String name;

		private MemInfo(String name) {
			this.name = name;
		}

		public static MemInfo getMemInfoByName(String name) {
			String searchName = StringUtils.trimToNull(name);
			for (MemInfo info : MemInfo.values()) {
				if (info.name.trim().equalsIgnoreCase(searchName)) {
					return info;
				}
			}
			return INVALID;
		}
	}

	public static final String SMAPS = "smaps";
	public static final int KB_TO_BYTES = 1024;
	private static final String KB = "kB";
	private static final String READ_ONLY_WITH_SHARED_PERMISSION = "r--s";
	private static final String READ_EXECUTE_WITH_SHARED_PERMISSION = "r-xs";
	private static final Pattern ADDRESS_PATTERN = Pattern
		.compile("([[a-f]|(0-9)]*)-([[a-f]|(0-9)]*)(\\s)*([rxwps\\-]*)");
	private static final Pattern MEM_INFO_PATTERN = Pattern
		.compile("(^[A-Z].*):[\\s ]*(\\d+).*");

	private boolean smapsEnabled;

	protected Map<String, ProcessTreeSmapMemInfo> processSMAPTree =
		new HashMap<String, ProcessTreeSmapMemInfo>();

	protected ProcessDiskInfo processDiskInfo;

	// to enable testing, using this variable which can be configured
	// to a test directory.
	private String procfsDir;

	static private String deadPid = "-1";
	private String pid = deadPid;
	static private Pattern numberPattern = Pattern.compile("[1-9][0-9]*");
	private long cpuTime = UNAVAILABLE;

	protected Map<String, ProcessInfo> processTree =
		new HashMap<String, ProcessInfo>();

	protected List<ProcessInfo> taskTree = new ArrayList<>();
	protected List<ProcessInfo> forstCoor = new ArrayList<>();
	protected List<ProcessInfo> forstRead = new ArrayList<>();
	protected List<ProcessInfo> forstWrite = new ArrayList<>();
	protected List<ProcessInfo> rocksLow = new ArrayList<>();
	protected List<ProcessInfo> rocksHigh = new ArrayList<>();

	public ProcfsBasedProcessTree() throws IOException {
		this(new File(PROCFS, SELF).getCanonicalFile().getName());
	}

	public ProcfsBasedProcessTree(boolean smapsEnabled) throws IOException {
		this(new File(PROCFS, SELF).getCanonicalFile().getName(), smapsEnabled);
	}

	public ProcfsBasedProcessTree(String pid) {
		this(pid, PROCFS);
	}

	public ProcfsBasedProcessTree(String pid, boolean smapsEnabled) {
		this(pid, PROCFS, SystemClock.getInstance(), smapsEnabled);
	}

	public ProcfsBasedProcessTree(String pid, String procfsDir) {
		this(pid, procfsDir, SystemClock.getInstance(), false);
	}

	/**
	 * Build a new process tree rooted at the pid.
	 * <p>
	 * This method is provided mainly for testing purposes, where
	 * the root of the proc file system can be adjusted.
	 *
	 * @param pid       root of the process tree
	 * @param procfsDir the root of a proc file system - only used for testing.
	 * @param clock     clock for controlling time for testing
	 */
	public ProcfsBasedProcessTree(String pid, String procfsDir, Clock clock, boolean smapsEnabled) {
		this.clock = clock;
		this.pid = getValidPID(pid);
		this.procfsDir = procfsDir;
		this.cpuTimeTracker = new CpuTimeTracker(JIFFY_LENGTH_IN_MILLIS);
		this.taskCpuTracker = new CpuTimeTracker(JIFFY_LENGTH_IN_MILLIS);
		this.forstCoorTracker = new CpuTimeTracker(JIFFY_LENGTH_IN_MILLIS);
		this.forstWriteTracker = new CpuTimeTracker(JIFFY_LENGTH_IN_MILLIS);
		this.forstReadTracker = new CpuTimeTracker(JIFFY_LENGTH_IN_MILLIS);
		this.rocksLowTracker = new CpuTimeTracker(JIFFY_LENGTH_IN_MILLIS);
		this.rocksHighTracker = new CpuTimeTracker(JIFFY_LENGTH_IN_MILLIS);
		this.smapsEnabled = smapsEnabled;
		this.processDiskInfo = new ProcessDiskInfo();
	}

	public void setSmapsEnabled(boolean smapsEnabled) {
		this.smapsEnabled = smapsEnabled;
	}

	/**
	 * Update process-tree with latest state. If the root-process is not alive,
	 * tree will be empty.
	 */
	public void updateProcessTree() {
		if (!pid.equals(deadPid)) {
			// Get the list of processes
			List<String> processList = getProcessList();

			Map<String, ProcessInfo> allProcessInfo = new HashMap<String, ProcessInfo>();

			// cache the processTree to get the age for processes
			Map<String, ProcessInfo> oldProcs =
				new HashMap<String, ProcessInfo>(processTree);
			processTree.clear();

			ProcessInfo me = null;
			for (String proc : processList) {
				// Get information for each process
				ProcessInfo pInfo = new ProcessInfo(proc);
				if (constructProcessInfo(pInfo, procfsDir) != null) {
					allProcessInfo.put(proc, pInfo);
					if (proc.equals(this.pid)) {
						me = pInfo; // cache 'me'
						processTree.put(proc, pInfo);
					}
				}
			}

			if (me == null) {
				return;
			}

			getTaskList(me.pid);

			// Add each process to its parent.
			for (Map.Entry<String, ProcessInfo> entry : allProcessInfo.entrySet()) {
				String pID = entry.getKey();
				if (!"1".equals(pID)) {
					ProcessInfo pInfo = entry.getValue();
					String ppid = pInfo.getPpid();
					// If parent is init and process is not session leader,
					// attach to sessionID
					if ("1".equals(ppid)) {
						String sid = pInfo.getSessionId().toString();
						if (!pID.equals(sid)) {
							ppid = sid;
						}
					}
					ProcessInfo parentPInfo = allProcessInfo.get(ppid);
					if (parentPInfo != null) {
						parentPInfo.addChild(pInfo);
					}
				}
			}

			// now start constructing the process-tree
			List<ProcessInfo> children = me.getChildren();
			Queue<ProcessInfo> pInfoQueue = new ArrayDeque<ProcessInfo>(children);
			while (!pInfoQueue.isEmpty()) {
				ProcessInfo pInfo = pInfoQueue.remove();
				if (!processTree.containsKey(pInfo.getPid())) {
					processTree.put(pInfo.getPid(), pInfo);
				}
				pInfoQueue.addAll(pInfo.getChildren());
			}

			// update age values and compute the number of jiffies since last update
			for (Map.Entry<String, ProcessInfo> procs : processTree.entrySet()) {
				ProcessInfo oldInfo = oldProcs.get(procs.getKey());
				if (procs.getValue() != null) {
					procs.getValue().updateJiffy(oldInfo);
					if (oldInfo != null) {
						procs.getValue().updateAge(oldInfo);
					}
				}
			}

			if (smapsEnabled) {
				// Update smaps info
				processSMAPTree.clear();
				for (ProcessInfo p : processTree.values()) {
					if (p != null) {
						// Get information for each process
						ProcessTreeSmapMemInfo memInfo = new ProcessTreeSmapMemInfo(p.getPid());
						constructProcessSMAPInfo(memInfo, procfsDir);
						processSMAPTree.put(p.getPid(), memInfo);
					}
				}
			}


			// update disk stat
			updateDiskStat();

		}
	}

	/**
	 * Verify that the given process id is same as its process group id.
	 *
	 * @return true if the process id matches else return false.
	 */
	public boolean checkPidPgrpidForMatch() {
		return checkPidPgrpidForMatch(pid, PROCFS);
	}

	public static boolean checkPidPgrpidForMatch(String _pid, String procfs) {
		// Get information for this process
		ProcessInfo pInfo = new ProcessInfo(_pid);
		pInfo = constructProcessInfo(pInfo, procfs);
		// null if process group leader finished execution; issue no warning
		// make sure that pid and its pgrpId match
		if (pInfo == null) {
			return true;
		}
		String pgrpId = pInfo.getPgrpId().toString();
		return pgrpId.equals(_pid);
	}

	private static final String PROCESSTREE_DUMP_FORMAT =
		"\t|- %s %s %d %d %s %d %d %d %d %s%n";

	public List<String> getCurrentProcessIDs() {
		return Collections.unmodifiableList(new ArrayList<>(processTree.keySet()));
	}

	/**
	 * Get a dump of the process-tree.
	 *
	 * @return a string concatenating the dump of information of all the processes
	 * in the process-tree
	 */
	public String getProcessTreeDump() {
		StringBuilder ret = new StringBuilder();
		// The header.
		ret.append(String.format("\t|- PID PPID PGRPID SESSID CMD_NAME "
			+ "USER_MODE_TIME(MILLIS) SYSTEM_TIME(MILLIS) VMEM_USAGE(BYTES) "
			+ "RSSMEM_USAGE(PAGES) FULL_CMD_LINE%n"));
		for (ProcessInfo p : processTree.values()) {
			if (p != null) {
				ret.append(String.format(PROCESSTREE_DUMP_FORMAT, p.getPid(), p
					.getPpid(), p.getPgrpId(), p.getSessionId(), p.getName(), p
					.getUtime(), p.getStime(), p.getVmem(), p.getRssmemPage(), p
					.getCmdLine(procfsDir)));
			}
		}
		return ret.toString();
	}

	public long getVirtualMemorySize() {
		return getVirtualMemorySize(0);
	}

	public long getVirtualMemorySize(int olderThanAge) {
		long total = 0L;
		boolean isAvailable = false;
		for (ProcessInfo p : processTree.values()) {
			if (p != null) {
				isAvailable = true;
				if (p.getAge() > olderThanAge) {
					total += p.getVmem();
				}
			}
		}
		return isAvailable ? total : UNAVAILABLE;
	}

	public long getRssMemorySize() {
		return getRssMemorySize(0);
	}

	public long getRssMemorySize(int olderThanAge) {
		if (PAGE_SIZE < 0) {
			return UNAVAILABLE;
		}
		if (smapsEnabled) {
			return getSmapBasedRssMemorySize(olderThanAge);
		}
		boolean isAvailable = false;
		long totalPages = 0;
		for (ProcessInfo p : processTree.values()) {
			if (p != null) {
				isAvailable = true;
				if (p.getAge() > olderThanAge) {
					totalPages += p.getRssmemPage();
				}
			}
		}
		return isAvailable ? totalPages * PAGE_SIZE : UNAVAILABLE; // convert # pages to byte
	}

	/**
	 * Get the resident set size (RSS) memory used by all the processes
	 * in the process-tree that are older than the passed in age. RSS is
	 * calculated based on SMAP information. Skip mappings with "r--s", "r-xs"
	 * permissions to get real RSS usage of the process.
	 *
	 * @param olderThanAge processes above this age are included in the memory addition
	 * @return rss memory used by the process-tree in bytes, for
	 * processes older than this age. return {@link #UNAVAILABLE} if it cannot
	 * be calculated.
	 */
	private long getSmapBasedRssMemorySize(int olderThanAge) {
		long total = UNAVAILABLE;
		for (ProcessInfo p : processTree.values()) {
			if (p != null) {
				// set resource to 0 instead of UNAVAILABLE
				if (total == UNAVAILABLE) {
					total = 0;
				}
				if (p.getAge() > olderThanAge) {
					ProcessTreeSmapMemInfo procMemInfo = processSMAPTree.get(p.getPid());
					if (procMemInfo != null) {
						for (ProcessSmapMemoryInfo info : procMemInfo.getMemoryInfoList()) {
							// Do not account for r--s or r-xs mappings
							if (info.getPermission().trim()
								.equalsIgnoreCase(READ_ONLY_WITH_SHARED_PERMISSION)
								|| info.getPermission().trim()
								.equalsIgnoreCase(READ_EXECUTE_WITH_SHARED_PERMISSION)) {
								continue;
							}

							// Account for anonymous to know the amount of
							// memory reclaimable by killing the process
							total += info.anonymous;

						}
					}
				}
			}
		}
		if (total > 0) {
			total *= KB_TO_BYTES; // convert to bytes
		}
		return total; // size
	}

	public long getCumulativeCpuTime() {
		if (JIFFY_LENGTH_IN_MILLIS < 0) {
			return UNAVAILABLE;
		}
		long incJiffies = 0;
		boolean isAvailable = false;
		for (ProcessInfo p : processTree.values()) {
			if (p != null) {
				// data is available
				isAvailable = true;
				incJiffies += p.getDtime();
			}
		}
		if (isAvailable) {
			// reset cpuTime to 0 instead of UNAVAILABLE
			if (cpuTime == UNAVAILABLE) {
				cpuTime = 0L;
			}
			cpuTime += incJiffies * JIFFY_LENGTH_IN_MILLIS;
		}
		return cpuTime;
	}

	private BigInteger getTotalProcessJiffies() {
		BigInteger totalStime = BigInteger.ZERO;
		long totalUtime = 0;
		for (ProcessInfo p : processTree.values()) {
			if (p != null) {
				totalUtime += p.getUtime();
				totalStime = totalStime.add(p.getStime());
			}
		}
		return totalStime.add(BigInteger.valueOf(totalUtime));
	}

	private static BigInteger getTotalTaskThreadJiffies(List<ProcessInfo> value) {
		BigInteger totalStime = BigInteger.ZERO;
		long totalUtime = 0;
		for (ProcessInfo p : value) {
			// LOG.info("Task Info: {} {} {} {}", p.getPid(), p.getName(), p.getStime(), p.getUtime());
			if (p != null) {
				totalUtime += p.getUtime();
				totalStime = totalStime.add(p.getStime());
			}
		}
		return totalStime.add(BigInteger.valueOf(totalUtime));
	}


	/**
	 * Get the CPU usage by all the processes in the process-tree in Unix.
	 * Note: UNAVAILABLE will be returned in case when CPU usage is not
	 * available. It is NOT advised to return any other error code.
	 *
	 * @return percentage CPU usage since the process-tree was created,
	 * {@link #UNAVAILABLE} if CPU usage cannot be calculated or not available.
	 */
	public float getCpuUsagePercent() {
		BigInteger processTotalJiffies = getTotalProcessJiffies();
		cpuTimeTracker.updateElapsedJiffies(processTotalJiffies,
			clock.absoluteTimeMillis());
		return cpuTimeTracker.getCpuTrackerUsagePercent();
	}

	public float getMainTaskCpu() {
		BigInteger processTotalJiffies = getTotalTaskThreadJiffies(taskTree);
		taskCpuTracker.updateElapsedJiffies(processTotalJiffies,
				clock.absoluteTimeMillis());
		return taskCpuTracker.getCpuTrackerUsagePercent();
	}

	public float getForstCoorCpu() {
        BigInteger processTotalJiffies = getTotalTaskThreadJiffies(forstCoor);
        forstCoorTracker.updateElapsedJiffies(processTotalJiffies,
                clock.absoluteTimeMillis());
        return forstCoorTracker.getCpuTrackerUsagePercent();
    }

	public float getForstWriteCpu() {
        BigInteger processTotalJiffies = getTotalTaskThreadJiffies(forstWrite);
        forstWriteTracker.updateElapsedJiffies(processTotalJiffies,
                clock.absoluteTimeMillis());
        return forstWriteTracker.getCpuTrackerUsagePercent();
    }

	public float getForstReadCpu() {
        BigInteger processTotalJiffies = getTotalTaskThreadJiffies(forstRead);
        forstReadTracker.updateElapsedJiffies(processTotalJiffies,
                clock.absoluteTimeMillis());
        return forstReadTracker.getCpuTrackerUsagePercent();
    }

	public float getRocksLowCpu() {
        BigInteger processTotalJiffies = getTotalTaskThreadJiffies(rocksLow);
        rocksLowTracker.updateElapsedJiffies(processTotalJiffies,
                clock.absoluteTimeMillis());
        return rocksLowTracker.getCpuTrackerUsagePercent();
    }

	public float getRocksHighCpu() {
        BigInteger processTotalJiffies = getTotalTaskThreadJiffies(rocksHigh);
        rocksHighTracker.updateElapsedJiffies(processTotalJiffies,
                clock.absoluteTimeMillis());
        return rocksHighTracker.getCpuTrackerUsagePercent();
    }

	private static String getValidPID(String pid) {
		if (pid == null) {
			return deadPid;
		}
		Matcher m = numberPattern.matcher(pid);
		if (m.matches()) {
			return pid;
		}
		return deadPid;
	}

	private void getTaskList(String pid) {
		taskTree.clear();
		forstRead.clear();
		forstWrite.clear();
		forstCoor.clear();
		rocksLow.clear();
		rocksHigh.clear();
		File taskDir = new File(procfsDir, pid + "/task");
		FileFilter procListFileFilter = new AndFileFilter(
				DirectoryFileFilter.INSTANCE, new RegexFileFilter(numberPattern));
		File[] taskList = taskDir.listFiles(procListFileFilter);
		if (ArrayUtils.isNotEmpty(taskList)) {
            for (File task : taskList) {
                File stat = new File(task, "stat");
				if (stat.exists()) {
					BufferedReader in = null;
					InputStreamReader fReader = null;
					try {
						fReader = new InputStreamReader(
								new FileInputStream(stat), Charset.forName("UTF-8"));
						in = new BufferedReader(fReader);
						String str = in.readLine();
						// LOG.info("task {}, {}", task.getName(), str);
						Matcher m = PROCFS_STAT_FILE_FORMAT.matcher(str);
						boolean mat = m.find();
						if (mat) {
							String processName = "(" + m.group(2) + ")";
							ProcessInfo pinfo = new ProcessInfo(task.getName());
							// Set (name) (ppid) (pgrpId) (session) (utime) (stime) (vsize) (rss)
							pinfo.updateProcessInfo(processName, m.group(3),
									Integer.parseInt(m.group(4)), Integer.parseInt(m.group(5)),
									Long.parseLong(m.group(7)), new BigInteger(m.group(8)),
									Long.parseLong(m.group(10)), Long.parseLong(m.group(11)));
							if (processName.startsWith("(Join")) {
								taskTree.add(pinfo);
							} else if (processName.startsWith("(ForSt-worker")) {
								forstRead.add(pinfo);
							} else if (processName.startsWith("(ForSt-write")) {
								forstWrite.add(pinfo);
                            } else if (processName.startsWith("(ForSt-Coor")) {
								forstCoor.add(pinfo);
							} else if (processName.startsWith("(rocksdb:low")) {
								rocksLow.add(pinfo);
							} else if (processName.startsWith("(rocksdb:high")) {
								rocksHigh.add(pinfo);
                            }
						}
					} catch (Exception f) {
						LOG.error(f.getMessage());
						// The process vanished in the interim!
					}
                }
            }
        }
	}

	/**
	 * Get the list of all processes in the system.
	 */
	private List<String> getProcessList() {
		List<String> processList = Collections.emptyList();
		FileFilter procListFileFilter = new AndFileFilter(
			DirectoryFileFilter.INSTANCE, new RegexFileFilter(numberPattern));

		File dir = new File(procfsDir);
		File[] processDirs = dir.listFiles(procListFileFilter);

		if (ArrayUtils.isNotEmpty(processDirs)) {
			processList = new ArrayList<String>(processDirs.length);
			for (File processDir : processDirs) {
				processList.add(processDir.getName());
			}
		}
		return processList;
	}

	/**
	 * Construct the ProcessInfo using the process' PID and procfs rooted at the
	 * specified directory and return the same. It is provided mainly to assist
	 * testing purposes.
	 * <p>
	 * Returns null on failing to read from procfs,
	 *
	 * @param pinfo     ProcessInfo that needs to be updated
	 * @param procfsDir root of the proc file system
	 * @return updated ProcessInfo, null on errors.
	 */
	private static ProcessInfo constructProcessInfo(ProcessInfo pinfo,
													String procfsDir) {
		ProcessInfo ret = null;
		// Read "procfsDir/<pid>/stat" file - typically /proc/<pid>/stat
		BufferedReader in = null;
		InputStreamReader fReader = null;
		try {
			File pidDir = new File(procfsDir, pinfo.getPid());
			fReader = new InputStreamReader(
				new FileInputStream(
					new File(pidDir, PROCFS_STAT_FILE)), Charset.forName("UTF-8"));
			in = new BufferedReader(fReader);
		} catch (FileNotFoundException f) {
			// The process vanished in the interim!
			return ret;
		}

		ret = pinfo;
		try {
			String str = in.readLine(); // only one line
			Matcher m = PROCFS_STAT_FILE_FORMAT.matcher(str);
			boolean mat = m.find();
			if (mat) {
				String processName = "(" + m.group(2) + ")";
				// Set (name) (ppid) (pgrpId) (session) (utime) (stime) (vsize) (rss)
				pinfo.updateProcessInfo(processName, m.group(3),
					Integer.parseInt(m.group(4)), Integer.parseInt(m.group(5)),
					Long.parseLong(m.group(7)), new BigInteger(m.group(8)),
					Long.parseLong(m.group(10)), Long.parseLong(m.group(11)));
			} else {
				ret = null;
			}
		} catch (IOException io) {
			ret = null;
		} finally {
			// Close the streams
			try {
				fReader.close();
				try {
					in.close();
				} catch (IOException i) {
				}
			} catch (IOException i) {
			}
		}

		return ret;
	}

	/**
	 * Returns a string printing PIDs of process present in the
	 * ProcfsBasedProcessTree. Output format : [pid pid ..]
	 */
	@Override
	public String toString() {
		StringBuffer pTree = new StringBuffer("[ ");
		for (String p : processTree.keySet()) {
			pTree.append(p);
			pTree.append(" ");
		}
		return pTree.substring(0, pTree.length()) + "]";
	}

	/**
	 * Returns boolean indicating whether pid
	 * is in process tree.
	 */
	public boolean contains(String pid) {
		return processTree.containsKey(pid);
	}

	/**
	 * Class containing information of a process.
	 */
	private static class ProcessInfo {
		private String pid; // process-id
		private String name; // command name
		private Integer pgrpId; // process group-id
		private String ppid; // parent process-id
		private Integer sessionId; // session-id
		private Long vmem; // virtual memory usage
		private Long rssmemPage; // rss memory usage in # of pages
		private Long utime = 0L; // # of jiffies in user mode
		private final BigInteger MAX_LONG = BigInteger.valueOf(Long.MAX_VALUE);
		private BigInteger stime = new BigInteger("0"); // # of jiffies in kernel mode
		// how many times has this process been seen alive
		private int age;

		// # of jiffies used since last update:
		private Long dtime = 0L;
		// dtime = (utime + stime) - (utimeOld + stimeOld)
		// We need this to compute the cumulative CPU time
		// because the subprocess may finish earlier than root process

		private List<ProcessInfo> children = new ArrayList<ProcessInfo>(); // list of children

		public ProcessInfo(String pid) {
			this.pid = pid;
			// seeing this the first time.
			this.age = 1;
		}

		public String getPid() {
			return pid;
		}

		public String getName() {
			return name;
		}

		public Integer getPgrpId() {
			return pgrpId;
		}

		public String getPpid() {
			return ppid;
		}

		public Integer getSessionId() {
			return sessionId;
		}

		public Long getVmem() {
			return vmem;
		}

		public Long getUtime() {
			return utime;
		}

		public BigInteger getStime() {
			return stime;
		}

		public Long getDtime() {
			return dtime;
		}

		public Long getRssmemPage() { // get rss # of pages
			return rssmemPage;
		}

		public int getAge() {
			return age;
		}

		public void updateProcessInfo(String name, String ppid, Integer pgrpId,
									  Integer sessionId, Long utime, BigInteger stime, Long vmem, Long rssmem) {
			this.name = name;
			this.ppid = ppid;
			this.pgrpId = pgrpId;
			this.sessionId = sessionId;
			this.utime = utime;
			this.stime = stime;
			this.vmem = vmem;
			this.rssmemPage = rssmem;
		}

		public void updateJiffy(ProcessInfo oldInfo) {
			if (oldInfo == null) {
				BigInteger sum = this.stime.add(BigInteger.valueOf(this.utime));
				if (sum.compareTo(MAX_LONG) > 0) {
					this.dtime = 0L;
				} else {
					this.dtime = sum.longValue();
				}
				return;
			}
			this.dtime = (this.utime - oldInfo.utime +
				this.stime.subtract(oldInfo.stime).longValue());
		}

		public void updateAge(ProcessInfo oldInfo) {
			this.age = oldInfo.age + 1;
		}

		public boolean addChild(ProcessInfo p) {
			return children.add(p);
		}

		public List<ProcessInfo> getChildren() {
			return children;
		}

		public String getCmdLine(String procfsDir) {
			String ret = "N/A";
			if (pid == null) {
				return ret;
			}
			BufferedReader in = null;
			InputStreamReader fReader = null;
			try {
				fReader = new InputStreamReader(
					new FileInputStream(
						new File(new File(procfsDir, pid.toString()), PROCFS_CMDLINE_FILE)),
					Charset.forName("UTF-8"));
			} catch (FileNotFoundException f) {
				// The process vanished in the interim!
				return ret;
			}

			in = new BufferedReader(fReader);

			try {
				ret = in.readLine(); // only one line
				if (ret == null) {
					ret = "N/A";
				} else {
					ret = ret.replace('\0', ' '); // Replace each null char with a space
					if (ret.isEmpty()) {
						// The cmdline might be empty because the process is swapped out or
						// is a zombie.
						ret = "N/A";
					}
				}
			} catch (IOException io) {
				ret = "N/A";
			} finally {
				// Close the streams
				try {
					fReader.close();
					try {
						in.close();
					} catch (IOException i) {
					}
				} catch (IOException i) {
				}
			}

			return ret;
		}
	}

	/**
	 * Update memory related information
	 *
	 * @param pInfo
	 * @param procfsDir
	 */
	private static void constructProcessSMAPInfo(ProcessTreeSmapMemInfo pInfo,
												 String procfsDir) {
		BufferedReader in = null;
		InputStreamReader fReader = null;
		try {
			File pidDir = new File(procfsDir, pInfo.getPid());
			File file = new File(pidDir, SMAPS);
			if (!file.exists()) {
				return;
			}
			fReader = new InputStreamReader(
				new FileInputStream(file), Charset.forName("UTF-8"));
			in = new BufferedReader(fReader);
			ProcessSmapMemoryInfo memoryMappingInfo = null;
			List<String> lines = IOUtils.readLines(in);
			for (String line : lines) {
				line = line.trim();
				try {
					Matcher address = ADDRESS_PATTERN.matcher(line);
					if (address.find()) {
						memoryMappingInfo = new ProcessSmapMemoryInfo(line);
						memoryMappingInfo.setPermission(address.group(4));
						pInfo.getMemoryInfoList().add(memoryMappingInfo);
						continue;
					}
					Matcher memInfo = MEM_INFO_PATTERN.matcher(line);
					if (memInfo.find()) {
						String key = memInfo.group(1).trim();
						String value = memInfo.group(2);

						if (memoryMappingInfo != null) {
							memoryMappingInfo.setMemInfo(key, value);
						}
					}
				} catch (Throwable t) {
				}
			}
		} catch (Throwable t) {
		} finally {
			IOUtils.closeQuietly(in);
		}
	}

	/**
	 * Placeholder for process's SMAPS information
	 */
	static class ProcessTreeSmapMemInfo {
		private String pid;
		private List<ProcessSmapMemoryInfo> memoryInfoList;

		public ProcessTreeSmapMemInfo(String pid) {
			this.pid = pid;
			this.memoryInfoList = new LinkedList<ProcessSmapMemoryInfo>();
		}

		public List<ProcessSmapMemoryInfo> getMemoryInfoList() {
			return memoryInfoList;
		}

		public String getPid() {
			return pid;
		}

		public String toString() {
			StringBuilder sb = new StringBuilder();
			for (ProcessSmapMemoryInfo info : memoryInfoList) {
				sb.append("\n");
				sb.append(info);
			}
			return sb.toString();
		}
	}

	/**
	 * <pre>
	 * Private Pages : Pages that were mapped only by the process
	 * Shared Pages : Pages that were shared with other processes
	 *
	 * Clean Pages : Pages that have not been modified since they were mapped
	 * Dirty Pages : Pages that have been modified since they were mapped
	 *
	 * Private RSS = Private Clean Pages + Private Dirty Pages
	 * Shared RSS = Shared Clean Pages + Shared Dirty Pages
	 * RSS = Private RSS + Shared RSS
	 * PSS = The count of all pages mapped uniquely by the process,
	 *  plus a fraction of each shared page, said fraction to be
	 *  proportional to the number of processes which have mapped the page.
	 *
	 * </pre>
	 */
	static class ProcessSmapMemoryInfo {
		private int size;
		private int rss;
		private int pss;
		private int sharedClean;
		private int sharedDirty;
		private int privateClean;
		private int privateDirty;
		private int anonymous;
		private int referenced;
		private String regionName;
		private String permission;

		public ProcessSmapMemoryInfo(String name) {
			this.regionName = name;
		}

		public String getName() {
			return regionName;
		}

		public void setPermission(String permission) {
			this.permission = permission;
		}

		public String getPermission() {
			return permission;
		}

		public int getSize() {
			return size;
		}

		public int getRss() {
			return rss;
		}

		public int getPss() {
			return pss;
		}

		public int getSharedClean() {
			return sharedClean;
		}

		public int getSharedDirty() {
			return sharedDirty;
		}

		public int getPrivateClean() {
			return privateClean;
		}

		public int getPrivateDirty() {
			return privateDirty;
		}

		public int getReferenced() {
			return referenced;
		}

		public int getAnonymous() {
			return anonymous;
		}

		public void setMemInfo(String key, String value) {
			MemInfo info = MemInfo.getMemInfoByName(key);
			int val = 0;
			try {
				val = Integer.parseInt(value.trim());
			} catch (NumberFormatException ne) {
				return;
			}
			if (info == null) {
				return;
			}
			switch (info) {
				case SIZE:
					size = val;
					break;
				case RSS:
					rss = val;
					break;
				case PSS:
					pss = val;
					break;
				case SHARED_CLEAN:
					sharedClean = val;
					break;
				case SHARED_DIRTY:
					sharedDirty = val;
					break;
				case PRIVATE_CLEAN:
					privateClean = val;
					break;
				case PRIVATE_DIRTY:
					privateDirty = val;
					break;
				case REFERENCED:
					referenced = val;
					break;
				case ANONYMOUS:
					anonymous = val;
					break;
				default:
					break;
			}
		}

		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb.append("\t").append(this.getName()).append("\n");
			sb.append("\t").append(MemInfo.SIZE.name + ":" + this.getSize())
				.append(" kB\n");
			sb.append("\t").append(MemInfo.PSS.name + ":" + this.getPss())
				.append(" kB\n");
			sb.append("\t").append(MemInfo.RSS.name + ":" + this.getRss())
				.append(" kB\n");
			sb.append("\t")
				.append(MemInfo.SHARED_CLEAN.name + ":" + this.getSharedClean())
				.append(" kB\n");
			sb.append("\t")
				.append(MemInfo.SHARED_DIRTY.name + ":" + this.getSharedDirty())
				.append(" kB\n");
			sb.append("\t")
				.append(MemInfo.PRIVATE_CLEAN.name + ":" + this.getPrivateClean())
				.append(" kB\n");
			sb.append("\t")
				.append(MemInfo.PRIVATE_DIRTY.name + ":" + this.getPrivateDirty())
				.append(" kB\n");
			sb.append("\t")
				.append(MemInfo.REFERENCED.name + ":" + this.getReferenced())
				.append(" kB\n");
			sb.append("\t")
				.append(MemInfo.ANONYMOUS.name + ":" + this.getAnonymous())
				.append(" kB\n");
			return sb.toString();
		}
	}

	public float getDiskTick() {
		return processDiskInfo.totTicks;
	}

	private void updateDiskStat() {
		// Read "/proc/diskstats" file
		BufferedReader in = null;
		InputStreamReader fReader = null;
		try {
			fReader = new InputStreamReader(
					new FileInputStream(
							new File("/proc/diskstats")), Charset.forName("UTF-8"));
			in = new BufferedReader(fReader);
		} catch (FileNotFoundException f) {
			LOG.info("File not found: /proc/diskstats");
		}

		try {
			in.readLine();
			in.readLine();
			// in.readLine();
			String str = in.readLine(); // use third line
			// LOG.info(str.trim().split("\\s+").length + " " + str);
			long ticks = Long.parseLong(str.trim().split("\\s+")[12]);
			long oldTicks = processDiskInfo.totTicks;
			long oldSampleTime = processDiskInfo.sampleTime;
			processDiskInfo.updateTotTicks(ticks, oldTicks, clock.absoluteTimeMillis(), oldSampleTime);
		} catch (IOException io) {

		} finally {
			// Close the streams
			try {
				fReader.close();
				try {
					in.close();
				} catch (IOException i) {
				}
			} catch (IOException i) {
			}
		}
	}

	/**
     * "/proc/diskstats", disk level information, all processes share disks
     */
	static class ProcessDiskInfo {
		String diskName;
		long rdIOs;
		long wrIOs;
		long totTicks;
		long deltaTotTicks;

		long sampleTime;

		long deltaSampleTime;

		public ProcessDiskInfo() {
            this.diskName = "empty";
			this.rdIOs = -1;
			this.wrIOs = -1;
			this.totTicks = -1;
			this.sampleTime = -1;
			this.deltaTotTicks = 0;
			this.deltaSampleTime = 0;
        }

		public void updateTotTicks(long newTicks, long oldTotTicks, long newTime, long oldSampleTime) {
			this.totTicks = newTicks;
			this.sampleTime = newTime;
			if (oldTotTicks == -1) {
				this.deltaSampleTime = 0;
				this.deltaTotTicks = 0;
				return;
			}
			if (sampleTime > oldSampleTime && totTicks >= oldTotTicks) {
				this.deltaTotTicks = this.totTicks - oldTotTicks;
				this.deltaSampleTime = this.sampleTime - oldSampleTime;
			}
        }

		public double getPercent() {
			if (this.deltaSampleTime == 0) {
                return 0;
            }
            return (float)this.deltaTotTicks / (float)this.deltaSampleTime;
		}

		@Override
		public String toString() {
			return "ProcessDiskInfo{diskName: " + diskName + ", rdIOs: "
					+ rdIOs + ", wrIOs: " + wrIOs + ", totTicks: " + totTicks + ", deltaTotTicks:" + deltaTotTicks + "}";
		}
	}

	/**
	 * Test the {@link ProcfsBasedProcessTree}
	 *
	 * @param args
	 */
	public static void main(String[] args) {
		if (args.length != 1) {
			System.out.println("Provide <pid of process to monitor>");
			return;
		}

		System.out.println("Creating ProcfsBasedProcessTree for process " +
			args[0]);
		ProcfsBasedProcessTree procfsBasedProcessTree = new ProcfsBasedProcessTree(args[0]);
		procfsBasedProcessTree.updateProcessTree();

		System.out.println(procfsBasedProcessTree.getProcessTreeDump());
		System.out.println("Get cpu usage " + procfsBasedProcessTree
			.getCpuUsagePercent());

		try {
			// Sleep so we can compute the CPU usage
			Thread.sleep(500L);
		} catch (InterruptedException e) {
			// do nothing
		}

		procfsBasedProcessTree.updateProcessTree();

		System.out.println(procfsBasedProcessTree.getProcessTreeDump());
		System.out.println("Cpu usage  " + procfsBasedProcessTree
			.getCpuUsagePercent());
		System.out.println("Vmem usage in bytes " + procfsBasedProcessTree
			.getVirtualMemorySize());
		System.out.println("Rss mem usage in bytes " + procfsBasedProcessTree
			.getRssMemorySize());
	}
}
