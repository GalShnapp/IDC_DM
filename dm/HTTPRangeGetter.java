import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.util.LinkedList;

/**
 * A runnable class which downloads a given url. It reads CHUNK_SIZE at a time
 * and writs it into a BlockingQueue. It supports downloading a range of data,
 * and limiting the download rate using a token bucket.
 */
public class HTTPRangeGetter implements Runnable {

    static final int CHUNK_SIZE = 4096; // constant
    static final int CONNECT_TIMEOUT = 500; // constant
    public static final int READ_TIMEOUT = 2000; // constant
    private Download session;
    private LinkedList<Range> ranges;
    private FileWriter fw;
    final String DCRLF = "\r\n\r\n";

    HTTPRangeGetter(Download session) {
        this.session = session;
        this.fw = session.getFileWriter();
        ranges = new LinkedList<Range>();
        setRanges();
    }

    /**
     * collects ranges from this {@link Download#rangeQueue}
     */
    private void setRanges() {
        boolean flag = true;
        while (flag == true) {
            Range rn = session.getRange();
            if (rn.isSignal()) {
                flag = false;
            } else {
                ranges.add(rn);
            }
        }
    }

    /**
     * 
     * @throws IOException
     * @throws InterruptedException
     * @throws SocketTimeoutException
     */
    private void downloadRange() throws IOException, InterruptedException, SocketTimeoutException {
        System.out.println("dbg: it is time to download!");
        URL url;
        InputStream strm;
        HttpURLConnection con;
        String rangeParamString = "";
        String boundary = "";

        if (ranges.size() == 0){
            session.signalDownloaderDone();
            return;
        }
        // generate request headrs
        for (Range range : ranges) {
            System.out.println("dbg: create params");
            rangeParamString += range.getStringParams();
            rangeParamString += ", ";
        }
        System.out.println("dbg " + rangeParamString);
        rangeParamString.substring(0, rangeParamString.length() - 2);
        
        

        // request
        url = session.getUrl();
        con = (HttpURLConnection) url.openConnection();
        con.setRequestMethod("GET");
        con.setConnectTimeout(CONNECT_TIMEOUT);
        con.setReadTimeout(READ_TIMEOUT);
        con.setRequestProperty("Range", rangeParamString);
        con.connect();

        // parse response
        int respCode = con.getResponseCode();
        if (respCode == 206) {
            String contentType = con.getContentType();
            int lastIndex = contentType.lastIndexOf("=");
            boundary = contentType.substring(lastIndex + 1, contentType.length());
        }
        int Clength = con.getContentLength();
        strm = (InputStream) con.getContent();
        // Range range = ranges.getFirst();

        for (Range range : ranges) {

            byte[] data = new byte[CHUNK_SIZE];
            int toRead = (int) range.getRemaining();
            if ( toRead < 0 || toRead > CHUNK_SIZE){
                toRead = CHUNK_SIZE;
            }
            System.out.println("dbg: printing range:");
            System.out.println(range);
            System.out.printf("b.len: %d, off: %d, len: %d \n",data.length,0,toRead);
            int bytesRead = strm.read(data, 0, toRead);
            //// String headerRegex = "Content-Range: bytes (\\d+)-(\\d+)/(\\d+)";
            //// Pattern r = Pattern.compile(headerRegex);
            String dataStr = new String(data);
            int index = dataStr.indexOf(DCRLF);
            String chunkData = dataStr.substring(index + DCRLF.length());
            Chunk ck = new Chunk(chunkData.getBytes(), range.getPOS() + range.getStart(), bytesRead);
            fw.pushToQueue(ck, range);
            while (!range.isComplete()) {
                data = new byte[CHUNK_SIZE];
                toRead = (int) range.getRemaining();
                if ( toRead < 0 || toRead > CHUNK_SIZE){
                    toRead = CHUNK_SIZE;
                }
                bytesRead = strm.read(data, 0, toRead);
                ck = new Chunk(data, range.getPOS() + range.getStart(), bytesRead);
                fw.pushToQueue(ck, range);
                if(range.getPOS() == 6545){
                    System.out.println(range.getEnd());
                    break;
                }
            }
        }
        con.disconnect();
        fw.pushToQueue(new Chunk(new byte[1], -1, -1), new Range(-1, -1, -1)); // Push an out of work
                                                                                              // flag
    }

    @Override
    public void run() {
        System.out.println("run of range getter");
        try {
            downloadRange();
        } catch (IOException | InterruptedException e) {
            System.err.println("Exception while fetching data from server: " + e);
        }
        System.out.println("downloader going to sleep");
    }
}
