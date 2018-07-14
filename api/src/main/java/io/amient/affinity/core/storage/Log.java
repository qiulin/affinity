package io.amient.affinity.core.storage;

import io.amient.affinity.core.util.EventTime;
import io.amient.affinity.core.util.MappedJavaFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class Log<POS extends Comparable<POS>> extends Thread implements Closeable {

    private enum FSM {
        INIT, BOOT, WRITE, TAIL
    }

    private final static Logger log = LoggerFactory.getLogger(Log.class);

    volatile private FSM fsm = FSM.INIT;

    final private AtomicReference<POS> checkpoint = new AtomicReference<>(null);

    volatile private boolean checkpointModified = false;

    volatile private boolean stopped = true;

    final private boolean enabled;
    final private Path checkpointFile;

    //TODO make checkpoint interval configurable
    final private int checkpointIntervalMs = 10000;

    final private LogStorage<POS> storage;

    private abstract class LogSync extends Thread implements Closeable { }

    private AtomicReference<LogSync> logsync = new AtomicReference<>();

    public Log(LogStorage<POS> storage, Path checkpointFile) {
        this.storage = storage;
        this.enabled = checkpointFile != null;
        this.checkpointFile = checkpointFile;
        if (enabled) {
            if (Files.exists(checkpointFile)) try {
                ObjectInputStream ois = new ObjectInputStream(new FileInputStream(checkpointFile.toFile()));
                try {
                    checkpoint.set((POS) ois.readObject());
                } finally {
                    ois.close();
                }
            } catch (Throwable e) {
                log.warn("Invalid checkpoint file: " + checkpointFile + ", going to rewind fully.", e);
                checkpoint.set(null);
            }
            log.info("Initialized " + checkpoint + " from " + checkpointFile);
        }
    }

    public POS getCheckpoint() {
        return checkpoint.get();
    }

    final private Consumer<Long> checkpointWriter = new Consumer<Long>() {
        private long lastWritten = 0L;
        @Override
        public void accept(Long time) {
            if (time <= 0L || lastWritten + checkpointIntervalMs < time) try {
                lastWritten = time;
                if (enabled && checkpointModified) {
                    POS position = checkpoint.get();
                    log.debug("Writing checkpoint " + position + " to file: " + checkpointFile);
                    ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(checkpointFile.toFile()));
                    oos.writeObject(position);
                    oos.close();
                    checkpointModified = false;
                }
            } catch (IOException e) {
                log.warn("Failed to update bootstrap checkpoint: ", e);
            }
        }
    };

    private void fsmEnterWriteState() {
        switch(fsm) {
            case INIT: throw new IllegalStateException("Bootstrap is required before writing");
            case TAIL: synchronized(this) {
                if (fsm == FSM.TAIL) stopLogSync();
                this.fsm = FSM.WRITE;
            }
            break;
            case BOOT: case WRITE: this.fsm = FSM.WRITE; break;
        }
    }

    public Future<POS> append(final MemStore kvstore, final byte[] key, byte[] valueBytes, final long recordTimestamp) {
        fsmEnterWriteState();
        Record record = new Record(key, valueBytes, recordTimestamp);
        return new MappedJavaFuture<POS, POS>(storage.append(record)) {
            @Override
            public POS map(POS position) {
                kvstore.put(ByteBuffer.wrap(key), kvstore.wrap(valueBytes, recordTimestamp));
                updateCheckpoint(position);
                return position;
            }
        };
    }

    public Future<POS> delete(final MemStore kvstore, final byte[] key) {
        fsmEnterWriteState();
        return new MappedJavaFuture<POS, POS>(storage.delete(key)) {
            @Override
            public POS map(POS position) {
                kvstore.remove(ByteBuffer.wrap(key));
                updateCheckpoint(position);
                return position;
            }
        };
    }

    public <K> long bootstrap(String identifier, final MemStore kvstore, int partition, Optional<ObservableState<K>> observableState) {
        switch(fsm) {
            case TAIL: stopLogSync(); break;
            case WRITE: flushWrites();  break;
            case INIT: case BOOT: break;
        }
        fsm = FSM.BOOT;

        POS checkpoint = getCheckpoint();
        long t = EventTime.unix();
        POS endOffset = storage.reset(partition, checkpoint);
        log.debug("Bootstrapping " + identifier + " from checkpoint:" + checkpoint + " to end-offset: " + endOffset);
        long numRecordsProcessed = 0L;
        if (endOffset != null) {
            Iterator<LogEntry<POS>> i = storage.boundedIterator();
            while (i.hasNext()) {
                LogEntry<POS> entry = i.next();
                if (checkpoint == null || entry.position.compareTo(checkpoint) > 0) {
                    modifyState(kvstore, entry, observableState);
                    numRecordsProcessed += 1;
                    checkpointWriter.accept(System.currentTimeMillis());
                }
            }
            updateCheckpoint(endOffset);
            checkpointWriter.accept(0L);
        }
        log.debug("Bootstrap completed: " + identifier + ", new checkpoint= " + getCheckpoint() +  ", duration.ms = " + (EventTime.unix() - t));
        return numRecordsProcessed;
    }

    public <K> void tail(final MemStore kvstore, Optional<ObservableState<K>> observableState) {
        switch(fsm) {
            case INIT: throw new IllegalStateException("Cannot transition from init to tail - bootstrap is required first");
            case WRITE: throw new IllegalStateException("Cannot transition from write to tail - bootstrap is required first");
            case BOOT: case TAIL: break;
        }
        fsm = FSM.TAIL;

        boolean success = logsync.compareAndSet(null, new LogSync() {
            @Override
            public void run() {
                try {
                    while (!isInterrupted()) {
                        Iterator<LogEntry<POS>> entries = storage.fetch(true);
                        if (entries == null) {
                            return;
                        } else while (entries.hasNext()) {
                            LogEntry<POS> entry = entries.next();
                            modifyState(kvstore, entry, observableState);
                        }
                    }
                } catch (Throwable e) {
                    log.error("Failure in the LogSync Thread", e);
                }
            }

            @Override
            public void close() throws IOException {
                storage.cancel();
                try {
                    log.trace("cancelling storage fetch operation and waiting for the logsync thread to complete..");
                    synchronized(this) {
                        join();
                    }
                } catch (InterruptedException e) {
                    throw new IOException(e);
                }
            }
        });
        if (!success) {
            throw new IllegalStateException("Another LogSync thread is already running");
        }
        logsync.get().start();
    }

    public void close() throws IOException {
        try {
            try {
                if (fsm == FSM.TAIL) stopLogSync();
            } finally {
                fsm = FSM.INIT;
                checkpointWriter.accept(0L);
            }
        } finally {
            stopped = true;
        }
    }

    @Override
    public void run() {
        try {
            stopped = false;
            while (!stopped) {
                TimeUnit.MILLISECONDS.sleep(checkpointIntervalMs);
                if (checkpointModified) checkpointWriter.accept(0L);
            }
        } catch (Exception e) {
            log.error("Error in the manager thread", e);
            Thread.currentThread().getThreadGroup().interrupt();
        }
    }

    private <K> void modifyState(MemStore kvstore, LogEntry<POS> entry, Optional<ObservableState<K>> observableState) {
        if (storage.isTombstone(entry)) {
            kvstore.remove(ByteBuffer.wrap(entry.key));
            observableState.ifPresent((state) -> state.internalPush(entry.key, Optional.empty()));
        } else {
            kvstore.put(ByteBuffer.wrap(entry.key), kvstore.wrap(entry.value, entry.timestamp));
            observableState.ifPresent((state) -> state.internalPush(entry.key, Optional.of(entry.value)));
        }
        updateCheckpoint(entry.position);
    }

    private void flushWrites() {
        storage.flush();
    }

    private void stopLogSync()  {
        LogSync sync = logsync.get();
        if (sync == null) {
            throw new IllegalStateException("Tail mode requires a running logsync thread");
        } else {
            logsync.compareAndSet(sync, null);
            try {
                sync.close();
            } catch (IOException e) {
                log.warn("could not close LogSync thread", e);
            }
        }
    }

    private POS updateCheckpoint(POS position) {
        return checkpoint.updateAndGet(chk -> {
            if (log.isTraceEnabled()) {
                log.trace("updating checkpoint, offset: " + position);
            }
            if (chk == null || position.compareTo(chk) > 0) {
                if (enabled) checkpointModified = true;
                return position;
            } else {
                return chk;
            }
        });
    }

}


