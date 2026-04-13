using System;
using System.Collections.Generic;
using System.Threading;
using Apache.Arrow;

namespace OpenEphys.Onix1.FrameWriter
{
    /// <summary>
    /// Provides buffered writing of items to an Arrow file, batching items before writing.
    /// </summary>
    /// <typeparam name="T">The type of items to be written and batched.</typeparam>
    public class ArrowBatchWriter<T> : ArrowWriter
    {
        readonly int bufferSize;
        readonly Func<IList<T>, Schema, RecordBatch> createRecordBatch;
        readonly Schema schema;

        List<T> buffer;

        readonly Timer timer;
        readonly object bufferLock = new();
        readonly TimeSpan timeout;

        int flushInProgress = 0;

        volatile bool disposed = false;

        /// <summary>
        /// Initializes a new instance of the ArrowBatchWriter class with the specified output file, schema, buffer
        /// size, and record batch creation delegate.
        /// </summary>
        /// <param name="filename">The path to the output file.</param>
        /// <param name="schema">The schema describing the structure of the data.</param>
        /// <param name="bufferSize">The maximum number of items to buffer before writing a batch.</param>
        /// <param name="timeout">The maximum time to wait before writing a batch.</param>
        /// <param name="createRecordBatch">A delegate to create a RecordBatch from a list of items and a schema.</param>
        public ArrowBatchWriter(string filename, Schema schema, int bufferSize, TimeSpan timeout, Func<IList<T>, Schema, RecordBatch> createRecordBatch)
            : base(filename, schema)
        {
            this.schema = schema;
            this.bufferSize = bufferSize;
            buffer = new(bufferSize);
            this.createRecordBatch = createRecordBatch;
            this.timeout = timeout;

            timer = new Timer(
                callback: TimerCallback,
                state: null,
                dueTime: timeout.Milliseconds,
                period: Timeout.Infinite);
        }

        void TimerCallback(object state)
        {
            if (disposed) return;
            Flush();
        }

        /// <summary>
        /// Adds an item to the buffer and flushes the buffer when it reaches its maximum size.
        /// </summary>
        /// <param name="item">The item to add to the buffer.</param>
        public void Write(T item)
        {
            if (disposed) return;

            bool shouldFlush = false;

            lock (bufferLock)
            {
                buffer.Add(item);

                if (buffer.Count >= bufferSize)
                {
                    shouldFlush = true;
                }
            }

            if (shouldFlush)
            {
                Flush();
            }
        }

        /// <summary>
        /// Writes any buffered records as a batch and clears the buffer.
        /// </summary>
        public void Flush()
        {
            if (Interlocked.CompareExchange(ref flushInProgress, 1, 0) != 0)
                return;

            timer.Change(timeout, Timeout.InfiniteTimeSpan);

            try
            {
                List<T> snapshot;

                lock (bufferLock)
                {
                    if (buffer.Count == 0) return;

                    snapshot = buffer;
                    buffer = new List<T>(bufferSize);
                }

                var recordBatch = createRecordBatch(snapshot, schema);
                base.Write(recordBatch);
            }
            finally
            {
                Interlocked.Exchange(ref flushInProgress, 0);
            }
        }

        /// <summary>
        /// Releases resources used by the object and flushes any buffered data.
        /// </summary>
        public override void Dispose()
        {
            if (!disposed)
            {
                timer.Dispose();
                Flush();
                base.Dispose();

                disposed = true;
            }
        }
    }
}
