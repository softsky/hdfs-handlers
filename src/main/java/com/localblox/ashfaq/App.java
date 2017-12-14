package com.localblox.ashfaq;

import com.localblox.ashfaq.filewatcher.HdfsFileWatcher;

/**
 *
 */
public class App {

    public static void main(String[] args) {

        // TODO approach #1: use own File watcher
        HdfsFileWatcher watcher = new HdfsFileWatcher(args[0]);

        watcher.start();

        // TODO approach #2: use Spark streaming API
        // ...

    }

}
