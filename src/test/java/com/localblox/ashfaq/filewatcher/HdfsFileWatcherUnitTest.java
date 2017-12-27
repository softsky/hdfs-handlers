package com.localblox.ashfaq.filewatcher;

import static com.localblox.ashfaq.filewatcher.HdfsFileWatcher.DEFAULT_IN_PATTERN;
import static com.localblox.ashfaq.filewatcher.HdfsFileWatcher.DEFAULT_OUT_PATTERN;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.localblox.ashfaq.action.NewFileInFolderActionAWSImpl;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.hdfs.DFSInotifyEventInputStream;
import org.apache.hadoop.hdfs.inotify.Event;
import org.apache.hadoop.hdfs.inotify.EventBatch;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

/**
 *
 */
@RunWith(JUnit4.class)
public class HdfsFileWatcherUnitTest {

    @Spy
    private HdfsFileWatcher fileWatcher = new HdfsFileWatcher("hdfs://tmp");

    @Mock
    private DFSInotifyEventInputStream inputStream;

    @Mock
    private EventBatch eventBatch;

    @Before
    public void init() throws Exception {

        MockitoAnnotations.initMocks(this);

        doReturn(inputStream).when(fileWatcher).getDfsInotifyEventInputStream();

        when(inputStream.poll(anyLong(), any())).thenAnswer(invke -> {
            Thread.sleep(1000);
            return eventBatch;
        });

        when(eventBatch.getEvents()).thenReturn(new Event[]{
            new Event.RenameEvent.Builder().dstPath("test").timestamp(System.currentTimeMillis()).build()
        });

    }

    @Test
    public void testStartStop() throws InterruptedException {

        Thread thread = new Thread(() -> fileWatcher.start());

        thread.start();

        Thread.sleep(4000);

        fileWatcher.stop();

        verify(fileWatcher, times(3)).processRenameEvent(any());
        // TODO verify logic was called

    }

    @Test
    public void testInOutRegexp() {

        String uuid = UUID.randomUUID().toString();

        String infile = "/in/" + uuid + ".csv";
        String outfile = "/out/" + uuid + ".csv";

        assertTrue(infile.matches(DEFAULT_IN_PATTERN));
        assertFalse(infile.matches(DEFAULT_OUT_PATTERN));

        assertTrue(outfile.matches(DEFAULT_OUT_PATTERN));
        assertFalse(outfile.matches(DEFAULT_IN_PATTERN));

    }
    // TODO - add more tests for processing

    @Test
    public void testSelectColumns() {

        String[] existing = new String[]{"A", "B", "C"};
        String[] desired = new String[]{"B", "C", "D"};

        String[] result = NewFileInFolderActionAWSImpl.getExistingDesiredColumns(existing, desired);

        System.out.println("REs = " + Arrays.toString(result));

        assertArrayEquals(new String[]{"B", "C"}, result);

    }

}
