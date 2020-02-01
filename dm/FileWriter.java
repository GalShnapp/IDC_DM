import java.util.concurrent.BlockingQueue;
import java.io.*;

/**
 * This class takes chunks from the queue, writes them to disk and updates the
 * file's metadata.
 */
public class FileWriter implements Runnable {

    private Download session; // the session this thread serves
    private BlockingQueue<Chunk> outQueue; // the queue to right to
    private File target; // the file to download
    private File mtd; // the metadata file
    RandomAccessFile raf; // for better accesses to the file

    /**
     * serves a {@link Download#Download}
     * writes chunks of data into the target file.
     * updates the metadata file
     * @param session
     */
    FileWriter(Download session) {
        this.session = session;
        this.outQueue = session.getOutQueue();
    }

    public boolean pushToQueue(Chunk ck, Range rn) {
        
        if (ck.isSignal()) { 
            // * this chunk contains no data, no need to parse anything
            try {
                outQueue.put(ck);
            } catch (InterruptedException ie) {
                System.err.println("error on pushing signal chunk");
            }
            return false;
        }
        
        try {
            outQueue.put(ck);
            rn.pushPOS(ck.getSize_in_bytes());
        } catch (InterruptedException ie) {
            System.err.println("error on pushing regular chunk");
        }
        return true;
    }
    
    private void writeChunks() throws IOException {
        int perc = session.getPercentage();
        System.out.println(perc + "%");
        try {
            while (!session.downloadDone()) {
                Chunk chunk = outQueue.take();
                if (chunk.isSignal()) {
                    session.signalDownloaderDone();
                } else {
                    byte[] data = chunk.getData();
                    long offset = chunk.getOffset();
                    raf.seek(offset);
                    raf.write(data);
                    session.pushWritten(chunk.getSize_in_bytes());
                    if (session.getPercentage() != perc){
                        perc = session.getPercentage();
                        System.out.println(perc + "%");
                    }
                }
            }
        } catch (InterruptedException ex) {
            System.out.println("Exception While Retrieving chunk from Q: " + ex);
        }
    }

    @Override
    public void run() {
        init();
        try {
            this.writeChunks();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("FILE WRITER DONE!");
    }
    
    /**
     * 
     */
    private void init(){
        try {
            target = new File(session.getFilename());
            raf = new RandomAccessFile(target, "rw");
            mtd = new File(session.getMetadataFilename());
            
            raf.setLength(session.getContentLength()); // examine this.
        } catch (NullPointerException npe) {
            System.err.println("file name is empty. Shutting down");
            System.err.println(npe.getMessage());
            IdcDm.usage();
        } catch (FileNotFoundException fnfe){
            System.err.println("file not found. Shutting down");
            System.err.println(fnfe.getMessage());
            IdcDm.usage();
        } catch (IOException ioe) {
            System.err.println("problem setting RAF length. Shutting down");
            System.err.println(ioe.getMessage());
            System.exit(0);
        }
        
    }
    
    
}
