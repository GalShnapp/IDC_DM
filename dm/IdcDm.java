import java.util.concurrent.*;

public class IdcDm {

    /**
     * Receive arguments from the command-line, provide some feedback and start the
     * download.
     *
     * @param args command-line arguments
     */
    public static void main(String[] args) {
        int numConnections = 1;

        if (args.length < 1 || args.length > 2) {
            usage();
        } else if (args.length == 2) {
            // recived a number of workers from user
            numConnections = Integer.parseInt(args[1]);
        }

        String url = args[0]; // TODO: parse better
                              // divide connections

        System.out.printf("Downloading");
        if (numConnections > 1) {
            System.out.printf(" using %d connections", numConnections);
        }
        System.out.printf("...\n");

        DownloadURL(url, numConnections);
        
        System.out.println("FINISH PROGRAM!");
    }

    /**
     * Initiate the file's metadata, and iterate over missing ranges. For each: 1.
     * Setup the Queue, Download session, FileWriter and a pool of HTTPRangeGetters.
     * Join the HTTPRangeGetters, send finish marker to the Queue and terminate the
     * TokenBucket 3. Join the FileWriter and RateLimiter
     *
     * Finally, print "Download succeeded/failed" and delete the metadata as needed.
     *
     * @param url            URL to download
     * @param numConnections number of concurrent connections
     */
    private static void DownloadURL(String url, int numConnections) {
        int numConsCounter = numConnections;
        Download session = new Download(url, numConnections);
        FileWriter fw = session.getFileWriter();
        Thread fileWriterThread = new Thread(fw);
        fileWriterThread.start();
        ThreadPoolExecutor workerThreadPool = (ThreadPoolExecutor) Executors.newFixedThreadPool(numConnections);
        for (int i = 0; i < numConnections; i++) {
            workerThreadPool.execute(new Thread(new HTTPRangeGetter(session)));
        }
        try {
            System.out.println("BEFORE AWAIT");
            workerThreadPool.shutdown();
            System.out.println("BEFORE JOIN");
            fileWriterThread.join();
            System.out.println("AFTER JOIN");
        } catch (InterruptedException ex) {
            System.err.println("Exception While Collecting Data: " + ex);
            ex.printStackTrace();
        }
        session.end();
        System.out.println("Download Finished!");

    }

    /**
     * prints the usage massege
     */
    public static void usage() {
        System.err.printf("usage:\n\tjava IdcDm URL|URL-LIST-FILE [MAX-CONCURRENT-CONNECTIONS]\n");
        System.exit(1);
    }

}
