package com.crunchydata.util;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.*;

/**
 * Utility class for logging operations.
 * Provides methods to initialize logging configurations and write log messages at various severity levels.
 *
 * <p>This class is not instantiable.</p>
 *
 * @author Brian Pace
 */
public class Logging {

    private static final Logger logger = Logger.getLogger(Logging.class.getName());
    private static final ThreadLocal<FileHandler> threadFileHandler = new ThreadLocal<>();
    private static final ConcurrentHashMap<Long, FileHandler> activeHandlers = new ConcurrentHashMap<>();

    static {
        System.setProperty("java.util.logging.SimpleFormatter.format", "[%1$tF %1$tT] [%4$-7s] %5$s %n");
    }

    /**
     * Initializes the Logging class with the provided properties.
     *
     * @param Props the properties to configure logging
     */
    public static void initialize(Properties Props) {

        // Set the log level based on the property value
        String logLevel = Props.getProperty("log-level", "INFO").toUpperCase();
        Level level = Level.parse(logLevel);
        logger.setLevel(level);

        // Configure file handler if log-destination is not stdout
        String logDestination = Props.getProperty("log-destination", "stdout");
        if (!"stdout".equalsIgnoreCase(logDestination)) {
            try {
                FileHandler fileHandler = new FileHandler(logDestination,0,1, true);
                fileHandler.setFormatter(new SimpleFormatter());
                logger.addHandler(fileHandler);
            } catch (Exception e) {
                System.out.println("Cannot allocate log file, will use stdout");
            }
        }
        System.out.println("初始化完成 - 当前logger级别：" + logger.getLevel());
    }

    public static void setThreadLogFile(String logFilePath) {
        closeThreadLogFile();

        try {
            File logFile = new File(logFilePath);
            File logDir = logFile.getParentFile();
            if (!logDir.exists()) {
                logDir.mkdirs();
            }

            // FileHandler参数: 问件大小1个G,仅保留1个文件,追加模式
            FileHandler fileHandler = new FileHandler(logFilePath, 1024 * 1024 * 1024, 1, true);
            fileHandler.setFormatter(new SimpleFormatter());
            fileHandler.setLevel(Level.ALL);

            logger.addHandler(fileHandler);
            threadFileHandler.set(fileHandler);
            activeHandlers.put(Thread.currentThread().getId(), fileHandler);
        } catch (IOException e) {
            System.err.println("无法创建日志文件处理器: " + e.getMessage());
        }
    }

    public static void closeThreadLogFile() {
        FileHandler handler = threadFileHandler.get();
        if (handler != null) {
            logger.removeHandler(handler);
            handler.close();
            threadFileHandler.remove();
            activeHandlers.remove(Thread.currentThread().getId());
        }
    }

    /**
     * Writes a log message at the specified severity level.
     *
     * @param severity the severity level of the log message (info, warning, severe)
     * @param module   the module where the log message originated
     * @param message  the log message
     */

    public static void write(String severity, String module, String message) {
        Level level;
        switch (severity.toLowerCase()) {
            case "info": level = Level.INFO; break;
            case "warning": level = Level.WARNING; break;
            case "severe": level = Level.SEVERE; break;
            case "config": level = Level.CONFIG; break;
            default: level = Level.FINE;
        }
        logger.log(level, "[{0}] {1}", new Object[]{module, message});
    }

}
