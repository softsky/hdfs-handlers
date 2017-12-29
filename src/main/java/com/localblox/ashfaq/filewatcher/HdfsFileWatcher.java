package com.localblox.ashfaq.filewatcher;

import com.localblox.ashfaq.action.NewFileInFolderActionAWSImpl;
import com.localblox.ashfaq.action.NewFileOutFolderActionAWSImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSInotifyEventInputStream;
import org.apache.hadoop.hdfs.client.HdfsAdmin;
import org.apache.hadoop.hdfs.inotify.Event;
import org.apache.hadoop.hdfs.inotify.Event.CloseEvent;
import org.apache.hadoop.hdfs.inotify.Event.RenameEvent;
import org.apache.hadoop.hdfs.inotify.EventBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * HDFS file watcher to react on file system events and process them.
 *
 * <br/> Inspired by https://stackoverflow.com/questions/29960186/hdfs-file-watcher
 *
 * <br/> Note that files in IN folder should match files in OUT folder by name. Each file should have unique name.
 *
 * <p/>
 * To prevent processing of partially uploaded file use inFilePattern and outFilePattern that must exclude uncomplete
 * file.
 *
 * <p/>
 * Basic file handling flow should be as follow:
 *
 * <br/> 1. Create file in folder IN or OUT. for example 'in/aaaaaa-bbbb-1234-5678.csv'
 *
 * <br/> 2. Write content to the file 'in/aaaaaa-bbbb-1234-5678.csv'
 *
 * <br/> 3. Close file. HDFS will fire CLOSE event to be processed by {@link HdfsFileWatcher}
 */
public class HdfsFileWatcher {

    private static final Logger log = LoggerFactory.getLogger(HdfsFileWatcher.class);

    // default file name as GUID
    static final String DEFAULT_IN_PATTERN = "\\/in\\/[0-9a-z\\-]{1,}\\.csv";
    // default file name as GUID
    static final String DEFAULT_OUT_PATTERN = "\\/out\\/[0-9a-z\\-]{1,}\\.csv";

    private String hdfsAdminUri;

    private String inFilePattern = DEFAULT_IN_PATTERN;

    private String outFilePattern = DEFAULT_OUT_PATTERN;

    /**
     * Creates HdfsFileWatcher instance.
     *
     * @param hdfsAdminUri - HDFS admin URI
     */
    public HdfsFileWatcher(final String hdfsAdminUri) {
        this.hdfsAdminUri = hdfsAdminUri;
    }

    /**
     * Creates HdfsFileWatcher instance.
     *
     * @param hdfsAdminUri   - HDFS admin URI
     * @param inFilePattern  - input file pattern for ready to process files
     * @param outFilePattern - output file pattern for ready to process file
     */
    public HdfsFileWatcher(final String hdfsAdminUri, final String inFilePattern, final String outFilePattern) {
        this.hdfsAdminUri = hdfsAdminUri;
        this.inFilePattern = inFilePattern;
        this.outFilePattern = outFilePattern;
    }

    // TODO check if processing will be in multiple thread. If so - use AtomicBoolean
    private AtomicBoolean proceed = new AtomicBoolean();

    /**
     * Start the watcher to process file.
     *
     * This method will block until processing is stopped.
     */
    //TODO - think about execution context inside Spark and blocking. May be run in separate thread if need.
    //TODO - if there should be more than one file watcher - resolve multiple file processing issue (one file
    // processed twice).
    public void start() {

        log.info("start HDFS watcher by hdfsAdminUri [{}]", hdfsAdminUri);

        proceed.set(true);

        DFSInotifyEventInputStream eventStream = getDfsInotifyEventInputStream();
        while (proceed.get()) {
            EventBatch events = null;
            log.info("proceed event listening: {}", proceed.get());
            try {
                // TODO - move to configuration.
                events = eventStream.poll(5L, TimeUnit.SECONDS);

                // TODO this is draft logic, need to be reviewed according to requirements
                processEventBatch(events);

            } catch (Exception e) {
                // TODO - handle exception.
                log.error("error while events watching: {}", e.getMessage(), e);
                try {
                    Thread.sleep(1000L);
                } catch (InterruptedException e1) {
                    log.error("thread interrupted: {}", e.getMessage());
                }
            }
        }

        log.info("shut down...");

    }

    /**
     * Process event batch
     *
     * @param events - events to be processed
     */
    void processEventBatch(final EventBatch events) {
        if (events != null) {
            for (Event event : events.getEvents()) {
                log.info("start process event type: {}, value: {}", event.getEventType(), event);
                switch (event.getEventType()) {
                    case RENAME:
                        processRenameEvent((RenameEvent) event);
                        break;
                    default:
                        //TODO - process default behaviour
                        break;
                }
                log.info("stop process event type: {}", event.getEventType());
            }
        }
    }

    /**
     * Process rename file event.
     *
     * @param renameEvent close event
     */
    void processRenameEvent(final RenameEvent renameEvent) {
        // TODO - we need to handle situation when there will be more that one file watcher.
        // And it should not take one file for processing more that once. So maube we need to rename file inprocess
        // (like locking)
        try {
            String filePath = renameEvent.getDstPath();

            if (filePath.matches(inFilePattern)) {
                new NewFileInFolderActionAWSImpl().doIt(filePath);
            } else if (filePath.matches(outFilePattern)) {
                new NewFileOutFolderActionAWSImpl().doIt(filePath);
            } else {
                log.warn("skip close event: file = {}, time = {} due to not match IN [{}] or OUT [{}] pattern",
                         filePath, renameEvent.getTimestamp(), inFilePattern, outFilePattern);
            }
        } catch (Exception e) {
            // TODO - handle file processing exception
            log.error("error of processing file", e);
        }
    }

    /**
     * Stop file event processing.
     *
     * Switches proceed flag to false. Actual stopping will be done on next cycle iteration.
     */
    public void stop() {
        proceed.set(false);
    }

    /**
     * Obtains {@link DFSInotifyEventInputStream} instance using hdfsAdminUri and empty {@link Configuration}.
     */
    DFSInotifyEventInputStream getDfsInotifyEventInputStream() {
        try {
            HdfsAdmin admin = new HdfsAdmin(URI.create(hdfsAdminUri), new Configuration());
            return admin.getInotifyEventStream();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
