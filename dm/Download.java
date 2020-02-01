import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.util.Arrays;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.io.*;

/**
 * Manages a single download.
 *
 */
class Download {

    FileWriter fw; // this session's file writer
    private String url_str; // the request from the user
    private String filename; // name of the file to be downloaded
    private int numConnections; // the number of connections as requested by the user
    private String metadataFilename; // mtd file name
    final BlockingQueue<Range> rangeQueue; // delivers downloadable ranges to {@link HTTPRangeGetter} threads
    final BlockingQueue<Chunk> outQueue; // holds the buffer for the output
    private URL url; // a url for the requested resource
    File mtdFile; // saves metadata to storage. helps for recovery
    private long contentLength; // size of the target file
    //private long written;
    private AtomicLong written;
    private boolean[] wasChunkDLed; // signals wether ot not the i'th chunk had been downloaded
                                    // we are limited to files that are smaller than 20GB
    public final int MAX_CONNECTIONS = 16; // seems like a reasonable cap.
    private AtomicInteger downloadersRemaining;

    /**
     * Manages a single download.
     * 
     * @param url            - url requested.
     * @param numConnections - number of concurrent connections to use
     */
    Download(String url, int numConnections) {
        this.url_str = url;
        this.numConnections = (numConnections > MAX_CONNECTIONS) ? MAX_CONNECTIONS : numConnections;
        this.written = new AtomicLong(0);
        this.contentLength = -1;
        this.filename = StripPath(url);
        this.metadataFilename = filename + ".mtd";
        this.outQueue = new LinkedBlockingQueue<Chunk>(1024); // generally seems like a good number powers of two are really convincing
        this.rangeQueue = new LinkedBlockingQueue<Range>(64); // generally seems like a good number powers of two are really convincing
        init();
    }

    public void dbg(Object s){
        System.out.println("dbg: " + s);
    }
    /**
     * inits everything. from metadata file if it exists or from scratch
     */
    void init() {
        this.fw = new FileWriter(this);
        downloadersRemaining = new AtomicInteger(numConnections);
        try {
            mtdFile = new File(metadataFilename);
            url = new URL(url_str);
            if (mtdFile.exists()) {
                initFromMtdFile();
            } else {
                initFromNothing();
            }
        } catch (NullPointerException npe) {
            System.err.println("null pointer to metadata file. Shutting down");
            IdcDm.usage();
        } catch (MalformedURLException mue) {
            System.err.println("cannot parse url. Shutting down");
            System.err.println(mue.getMessage());
            IdcDm.usage();
        }
    }

    /**
     * @return {@link Download#fw}
     */
    public FileWriter getFileWriter() {
        return this.fw;
    }

    /**
     * @return {@link Download#filename}
     */
    public String getFilename() {
        return this.filename;
    }

    /**
     * @return {@link Download#contentLength}
     */
    public long getContentLength() {
        return this.contentLength;
    }

    /**
     * @return {@link Download#metadataFilename}
     */
    public String getMetadataFilename() {
        return this.metadataFilename;
    }

    /**
     * @return {@link Download#outQueue}
     */
    public BlockingQueue<Chunk> getOutQueue() {
        return this.outQueue;
    }



    /**
     * 
     * @return {@link Download#url}
     */
    public URL getUrl() {
        return url;
    }

    /**
     * @return a single range from the range queue
     */
    public synchronized Range getRange() {
        return rangeQueue.poll();
    }

    /**
     * strips the preceeding path (local or remote)
     * 
     * @param path path/link
     * @return the name of the file requested by the user
     */
    private static String StripPath(String path) {
        return path.substring(path.lastIndexOf('/') + 1, path.length());
    }

    /**
     * gets header info from remote server, init local variables and ranges
     */
    private void initFromNothing() {
        queryForHeaders();
        int chucnksInDl = (int) contentLength/HTTPRangeGetter.CHUNK_SIZE;
        wasChunkDLed = new boolean[10000000];
        written.set(0);
        Arrays.fill(wasChunkDLed, false); // no chunk was downloaded
        long rnSize = contentLength / numConnections;
        for (int i = 0; i < numConnections; i++) {
            // create a range for each thread to get
            if (i+1 == numConnections) {
                rangeQueue.add(new Range(i * rnSize, contentLength -1  , i * rnSize ));
                break;
            } 
            rangeQueue.add(new Range(i * rnSize , (i + 1) * rnSize - 1, i * rnSize));
        }
        for (int i = 0; i < numConnections; i++) {
            rangeQueue.add(new Range(-1, -1,-1));
        }
    }

    /**
     * queries the server for the downloads headers
     */
    private void queryForHeaders() {
        HttpURLConnection con;
        int respCode;
        try {
            con = (HttpURLConnection) url.openConnection();
            con.setRequestMethod("GET");
            con.setConnectTimeout(HTTPRangeGetter.CONNECT_TIMEOUT);
            con.connect();

            respCode = con.getResponseCode();

            switch (respCode / 100) {
            case 1: // nope
                break;
            case 2: // ok status
                break;
            case 3: // shouldn't be. java defaults to follow redirections
                break;
            case 4: // client error
                System.err.print("Request came back with status code: " + respCode);
                System.err.println(", Try fixing your link. shutting down");
                IdcDm.usage();
                ;
                break;
            case 5: // server error
                System.err.print("Request came back with status code: " + respCode);
                System.err.println(", This is due to some server error and you should try again soon. Shutting down");
                IdcDm.usage();
            default:
                IdcDm.usage();
                break;
            }

            this.contentLength = con.getContentLengthLong(); // "assume this is good"
            System.out.println("cont length: " + contentLength);
            con.disconnect();
        } catch (SocketTimeoutException ste) {
            System.err.println("initial connection timed out. Shutting down");
            System.err.println(ste.getMessage());
        } catch (IOException ie) {
            System.err.println("expirienced an error during initial connection. Shutting down");
            System.err.println(ie.getMessage());
        }
    }

    public void end(){

    }
    /**************** recovery ****************/

    /**
     * an object containing all the data needed for a session. implements
     * {@link Serializable} for presistence read by Download and written by
     * FileWriter
     */
    public class MtdFileOpaque implements Serializable {

        /**
         *
         */
        private static final long serialVersionUID = 1L;
        long contentLength;
        long written;
        boolean[] wasChunkDLed;

        MtdFileOpaque() {
            written = -1;
            contentLength = -1;
        }

        MtdFileOpaque(long cl, long w, boolean[] wcdl) {
            contentLength = cl;
            written = w;
            wasChunkDLed = wcdl;
        }
    }

    /**
     * reads the metadata file. inits local variables, ranges and opaque
     */
    private void initFromMtdFile() {
        MtdFileOpaque mfo = new MtdFileOpaque();
        FileInputStream fis;
        ObjectInputStream ois; // used to read metadata file
        try {
            fis = new FileInputStream(mtdFile);
            ois = new ObjectInputStream(fis);
            mfo = (MtdFileOpaque) ois.readObject();
            ois.close();
            fis.close();

            this.contentLength = mfo.contentLength;
            this.written.set(mfo.written);
            this.wasChunkDLed = mfo.wasChunkDLed.clone();

        } catch (FileNotFoundException fnfe) {
            System.err.println("Metadata file could not be opened. Shutting down.");
            System.exit(0);
        } catch (IOException ie) {
            System.err.println("Metadata file could not be read. Shutting down.");
            System.exit(0);
        } catch (ClassNotFoundException cnfe) {
            System.err.println("Metadata file could not be read. Shutting down.");
            System.exit(0);
        }
        parseMtdArray();
    }

    /**
     * goes over the array, and creates a Range for each segment
     */
    private void parseMtdArray() {
        int s = 0; // stores the starting index for a segment
        final boolean START = false, END = true;
        boolean lookingFor = START;
        long fByte = 0, lByte = 0; // first and last byte
        for (int i = 0; i < wasChunkDLed.length; i++) {
            if (wasChunkDLed[i] == lookingFor) {
                if (lookingFor == START) {
                    lookingFor = END;
                    s = i;
                } else { // lookingFor == end
                    lookingFor = START;
                    // translate chunks to bytes
                    fByte = (long) s * HTTPRangeGetter.CHUNK_SIZE;
                    lByte = (long) i * HTTPRangeGetter.CHUNK_SIZE;
                    lByte--;
                    rangeQueue.add(new Range(fByte, lByte, fByte));
                }
            }
        }
    }

    /************** status calls **************/

    public boolean downloadDone() {
        return ((written.get() == this.contentLength -1 ) && allDownloadersDone());
    }

    public boolean allDownloadersDone() {
        return downloadersRemaining.get() == 0;
    }

    public int signalDownloaderDone() {
        System.out.println("downloader singaled done");
        System.out.println("There are: " + downloadersRemaining.get() + "downloaders");
        int a = downloadersRemaining.get();
        int b = downloadersRemaining.decrementAndGet();
        System.out.println("before: " + a + "after: " + b);
        return b;
    }

    public void pushWritten(long written) {
        long l = this.written.getAndAdd(written);
        // System.out.println("written: " + l);
    }

    public boolean rangeQueueEmpty(){
        return this.rangeQueue.isEmpty();
    }

    public long getWritten(){
        return this.written.get();
    }

        /**
     * @return percentage of data downloaded
     */
    public int getPercentage() {
        return (int) ((100 * written.get()) / contentLength);
    }
   
}