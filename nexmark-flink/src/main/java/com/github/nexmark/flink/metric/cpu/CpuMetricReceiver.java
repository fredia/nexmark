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

import org.apache.flink.configuration.Configuration;

import com.github.nexmark.flink.utils.NexmarkGlobalConfiguration;
import com.github.nexmark.flink.FlinkNexmarkOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.github.nexmark.flink.metric.cpu.CpuMetricSender.DELIMITER;

public class CpuMetricReceiver implements Closeable {

	private static final Logger LOG = LoggerFactory.getLogger(CpuMetricReceiver.class);

	/**
	 * Server socket to listen at.
	 */
	private final ServerSocket server;

	private final ConcurrentHashMap<String, Double> cpuMetrics = new ConcurrentHashMap<>();

	public final ConcurrentHashMap<String, Double> taskCpuMetrics = new ConcurrentHashMap<>();

	public final ConcurrentHashMap<String, Double> forstCoorMetrics = new ConcurrentHashMap<>();

	public final ConcurrentHashMap<String, Double> forstReadMetrics = new ConcurrentHashMap<>();

	public final ConcurrentHashMap<String, Double> forstWriteMetrics = new ConcurrentHashMap<>();

	public final ConcurrentHashMap<String, Double> rocksdbLowMetrics = new ConcurrentHashMap<>();

	public final ConcurrentHashMap<String, Double> rocksdbHighMetrics = new ConcurrentHashMap<>();

	public final ConcurrentHashMap<String, Double> ioUtilMetrics = new ConcurrentHashMap<>();


	private final ExecutorService service = Executors.newCachedThreadPool();

	public CpuMetricReceiver(String host, int port) {
		try {
			InetAddress address = InetAddress.getByName(host);
			server = new ServerSocket(port, 10, address);
		} catch (IOException e) {
			throw new RuntimeException("Could not open socket to receive back cpu metrics.", e);
		}
	}

	public void runServer() {
		service.submit(this::runServerBlocking);
	}

	public void runServerBlocking() {
		try {
			//noinspection InfiniteLoopStatement
			while (true) {
				Socket socket = server.accept();
				service.submit(new ServerThread(socket, cpuMetrics, taskCpuMetrics,
						forstCoorMetrics, forstReadMetrics, forstWriteMetrics, rocksdbLowMetrics, rocksdbHighMetrics,
						ioUtilMetrics));
			}
		} catch (IOException e) {
			LOG.error("Failed to start the socket server.", e);
			try {
				server.close();
			} catch (Throwable ignored) {
			}
		}
	}

	public double getTotalCpu() {
		double sumCpu = 0.0;
		int size = 0;
		for (Double cpu : cpuMetrics.values()) {
			size++;
			sumCpu += cpu;
		}
		if (size == 0) {
			LOG.warn("The cpu metric receiver doesn't receive any metrics.");
		}
		return sumCpu;
	}

	public int getNumberOfTM() {
		return cpuMetrics.size();
	}

	public void printAll() {
		System.out.println("=============== IOUtil ===============");
		for (Map.Entry<String, Double> entry : ioUtilMetrics.entrySet()) {
			System.out.println(String.format("Host %s, IOUtil: %f", entry.getKey(), entry.getValue()));
        }
		System.out.println(String.format("taskCpu=%f, forstCoor=%f, forstRead=%f, forstWrite=%f, rocksLow=%f, rocksHigh=%f",
				getTotalMetric(taskCpuMetrics), getTotalMetric(forstCoorMetrics), getTotalMetric(forstReadMetrics), getTotalMetric(forstWriteMetrics), getTotalMetric(rocksdbLowMetrics), getTotalMetric(rocksdbHighMetrics)));
	}

	public double getTotalIOUtil() {
		double sumIOUtil = 0.0;
        int size = 0;
        for (Double ioUtil : ioUtilMetrics.values()) {
            size++;
            sumIOUtil += ioUtil;
        }
        if (size == 0) {
            LOG.warn("The ioUtil metric receiver doesn't receive any metrics.");
        }
        return sumIOUtil;
	}

	public double getTotalMetric(Map<String, Double> metrics) {
		double sumTaskCpu = 0.0;
        int size = 0;
        for (Double taskCpu : metrics.values()) {
            size++;
            sumTaskCpu += taskCpu;
        }
        if (size == 0) {
            LOG.warn("The taskCpu metric receiver doesn't receive any metrics.");
        }
        return sumTaskCpu;
	}

	@Override
	public void close() {
		try {
			server.close();
		} catch (Throwable ignored) {
		}

		service.shutdownNow();
	}

	private static final class ServerThread implements Runnable {

		private final Socket socket;
		private final ConcurrentHashMap<String, Double> cpuMetrics;

		private final ConcurrentHashMap<String, Double> taskCpuMetrics;

		private final ConcurrentHashMap<String, Double> forstCoorMetrics;

		private final ConcurrentHashMap<String, Double> forstReadMetrics;

		private final ConcurrentHashMap<String, Double> forstWriteMetrics;

		private final ConcurrentHashMap<String, Double> rocksdbLowMetrics;

		private final ConcurrentHashMap<String, Double> rocksdbHighMetrics;

		private final ConcurrentHashMap<String, Double> ioUtilMetrics;


		private ServerThread(Socket socket, ConcurrentHashMap<String, Double> cpuMetrics,
							 ConcurrentHashMap<String, Double> taskCpuMetrics,
							 ConcurrentHashMap<String, Double> forstCoorMetrics,
							 ConcurrentHashMap<String, Double> forstReadMetrics,
							 ConcurrentHashMap<String, Double> forstWriteMetrics,
							 ConcurrentHashMap<String, Double> rocksdbLowMetrics,
                             ConcurrentHashMap<String, Double> rocksdbHighMetrics,
							 ConcurrentHashMap<String, Double> ioUtilMetrics) {
			this.socket = socket;
			this.cpuMetrics = cpuMetrics;
			this.taskCpuMetrics = taskCpuMetrics;
			this.forstCoorMetrics = forstCoorMetrics;
			this.forstReadMetrics = forstReadMetrics;
            this.forstWriteMetrics = forstWriteMetrics;
            this.rocksdbLowMetrics = rocksdbLowMetrics;
            this.rocksdbHighMetrics = rocksdbHighMetrics;
			this.ioUtilMetrics = ioUtilMetrics;
		}

		@Override
		public void run() {
			try {
				InputStream inStream = socket.getInputStream();
				ByteArrayOutputStream buffer = new ByteArrayOutputStream();
				int b;
				while ((b = inStream.read()) >= 0) {
					// buffer until delimiter
					if (b != DELIMITER) {
						buffer.write(b);
					}
					// decode and emit record
					else {
						byte[] bytes = buffer.toByteArray();
						String message = new String(bytes, StandardCharsets.UTF_8);
						LOG.info("Received CPU metric report: {}", message);
						List<CpuMetric> receivedMetrics = CpuMetric.fromJsonArray(message);
						for (CpuMetric metric : receivedMetrics) {
							cpuMetrics.put(metric.getHost() + ":" + metric.getPid(), metric.getCpu());
							taskCpuMetrics.put(metric.getHost() + ":" + metric.getPid(), metric.getTaskCpu());
							forstCoorMetrics.put(metric.getHost() + ":" + metric.getPid(), metric.getForstCoorCpu());
							forstReadMetrics.put(metric.getHost() + ":" + metric.getPid(), metric.getForstReadCpu());
                            forstWriteMetrics.put(metric.getHost() + ":" + metric.getPid(), metric.getForstWriteCpu());
                            rocksdbLowMetrics.put(metric.getHost() + ":" + metric.getPid(), metric.getRocksdbLowCpu());
                            rocksdbHighMetrics.put(metric.getHost() + ":" + metric.getPid(), metric.getRocksdbHighCpu());
							ioUtilMetrics.put(metric.getHost() + ":" + metric.getPid(), metric.getIoUtil());
						}
						buffer.reset();
					}
				}
			} catch (IOException e) {
				LOG.error("Socket server error.", e);
			} finally {
				try {
					socket.close();
				} catch (IOException ex) {
					// ignore
				}
			}
		}
	}

	public static void main(String[] args) throws ExecutionException, InterruptedException {
		// start metric servers
		Configuration conf = NexmarkGlobalConfiguration.loadConfiguration();
		String reporterAddress = conf.get(FlinkNexmarkOptions.METRIC_REPORTER_HOST);
		int reporterPort = conf.get(FlinkNexmarkOptions.METRIC_REPORTER_PORT);
		CpuMetricReceiver cpuMetricReceiver = new CpuMetricReceiver(reporterAddress, reporterPort);
		cpuMetricReceiver.runServer();
		cpuMetricReceiver.service.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
	}
}
