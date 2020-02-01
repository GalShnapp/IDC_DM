import java.io.Serializable;
import static java.lang.Math.toIntExact;

/**
 * Describes a simple range, with a start, an end, and a length
 */
class Range implements Serializable {
    final private long start; // first byte
    private long end; // last byte
    private long pos; //
    final private long len;

    Range(long start, long end, long pos) {
        this.start = start;
        this.end = end;
        this.pos = pos;
        this.len = end - start;
    }

    public long getPOS() {
        return pos;
    }

    public long getRemaining() {
        return end - (pos + start);
    }

    public long getLength() {
        return len;
    }

    public void pushPOS(int delta) {
        this.pos += delta;
    }

    public boolean isComplete() {
        return pos == len;
    }

    Long getStart() {
        return start;
    }

    Long getEnd() {
        return end;
    }

    String getStringParams() {
        return  start + "-" + end;
    }

    public void setLast(long end) {
        this.end = end;
    }

    public String toString(){
        String s = "start: " + this.start;
        s += "\n pos: " + this.pos;
        s += "\n end: " + this.end;
        s += "\n len: " + this.len;
        return s;
    }

    public boolean isSignal(){
        return this.start == -1;
    }
}
