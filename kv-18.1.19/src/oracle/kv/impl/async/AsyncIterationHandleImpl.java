/*-
 * Copyright (C) 2011, 2018 Oracle and/or its affiliates. All rights reserved.
 *
 * This file was distributed by Oracle as part of a version of Oracle NoSQL
 * Database made available at:
 *
 * http://www.oracle.com/technetwork/database/database-technologies/nosqldb/downloads/index.html
 *
 * Please see the LICENSE file included in the top-level directory of the
 * appropriate version of Oracle NoSQL Database for a copy of the license and
 * additional information.
 */

package oracle.kv.impl.async;

import static oracle.kv.impl.util.ObjectUtil.checkNull;

import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.AsyncIterationHandle;
import oracle.kv.CompletionHandler;
import oracle.kv.ResultHandler;
import oracle.kv.stats.DetailedMetrics;

/**
 * An implementation of {@link AsyncIterationHandle}.
 *
 * @param <E> the type of the iteration element
 */
public class AsyncIterationHandleImpl<E>
        implements AsyncIterationHandle<E>, IterationHandleNotifier {

    /**
     * A thread local variable that records whether a call to the request
     * method is already underway in the current thread.
     */
    private final ThreadLocal<Boolean> inRequest = new ThreadLocal<Boolean>();

    /**
     * The logger.
     */
    private final Logger logger;

    /**
     * The object used for locking, to avoid interference from application code
     * that could lock this instance.  Non-final fields should only be accessed
     * by using synchronization on this lock, either from within a synchronized
     * block, in a code block that is running exclusively because it set
     * notifyingHandler while synchronized, or, for fields that are only set
     * once to a non-null value, after checking for a non-null value while
     * synchronized.  The newNotify field is special since it is set by
     * non-notifying threads: that field should only be read while in a
     * synchronized block.
     */
    private final Object lock = new Object();

    /**
     * The async iterator that supplies locally available values and transmits
     * close requests, or null if not initialized.  Set only once.
     */
    private AsyncTableIterator<E> asyncIterator;

    /**
     * The next handler or null if not initialized.  Set only once and only if
     * asyncIterator is also set.
     */
    private ResultHandler<E> nextHandler;

    /**
     * The completion handler or null if not initialized.  Set only once and
     * only if asyncIterator is also set.
     */
    private CompletionHandler completionHandler;

    /** The number of iteration items requested. */
    private long requests;

    /** Used to make sure only one party notifies handlers at a time. */
    private boolean notifyingHandler;

    /**
     * Set when notifyNext is called while another thread is notifying
     * handlers.  If true, the notifying thread will check again for a next
     * element or whether the iteration is closed before exiting.
     */
    private boolean newNotify;

    /** Whether the completion handler has been notified. */
    private boolean calledOnComplete;

    /** Whether cancel has been called.  Set only once. */
    private boolean cancelCalled;

    /**
     * Creates an instance of this class.
     */
    public AsyncIterationHandleImpl(Logger logger) {
        this.logger = checkNull("logger", logger);
    }

    /**
     * Sets the iterator that the handle should use to obtain elements.  This
     * method is typically called in the iterator constructor, and must be
     * called before the iterate method is called.
     *
     * @param asyncIterator the iterator
     * @throws IllegalStateException if the iterator has already been specified
     */
    public void setIterator(AsyncTableIterator<E> asyncIterator) {
        synchronized (lock) {
            if (this.asyncIterator != null) {
                throw new IllegalStateException(
                    "The iterator has already been specified");
            }
            this.asyncIterator = checkNull("asyncIterator", asyncIterator);
        }
    }

    /* -- Implement AsyncIterationHandle -- */

    @Override
    public void iterate(ResultHandler<E> next, CompletionHandler completion) {
        checkNull("next", next);
        checkNull("completion", completion);
        synchronized (lock) {
            if (asyncIterator == null) {
                throw new IllegalStateException("Internal error: no iterator");
            }
            if (this.nextHandler != null) {
                throw new IllegalStateException(
                    "The iterate method has already been called");
            }
            nextHandler = next;
            completionHandler = completion;
        }

        /*
         * Notify handlers in case something happened before the handlers were
         * set.
         */
        notifyNext();
    }

    @Override
    public void request(long n) {
        if (n < 1) {
            throw new IllegalArgumentException(
                "Request value must be greater than zero");
        }
        synchronized (lock) {
            if (asyncIterator == null) {
                throw new IllegalStateException("Internal error: no iterator");
            }

            requests += n;

            /* Check for overflow */
            if (requests <= 0) {
                requests = Long.MAX_VALUE;
            }
        }

        /*
         * If we are already within a nested call to the request method, then
         * the top level call will deliver the results.  A recursive call can
         * happen if the application calls request, request calls notifyNext,
         * that calls the application's next result handler's onResult method,
         * and that method calls request again.
         */
        if (inRequest.get() != null) {
            return;
        }
        inRequest.set(Boolean.TRUE);
        try {
            notifyNext();
        } finally {
            inRequest.remove();
        }
    }

    @Override
    public void cancel() {
        synchronized (lock) {
            if (asyncIterator == null) {
                throw new IllegalStateException("Internal error: no iterator");
            }
            if (cancelCalled) {
                return;
            }
            cancelCalled = true;
        }
        notifyNext();
    }

    @Override
    public List<DetailedMetrics> getPartitionMetrics() {
        synchronized (lock) {
            if (asyncIterator == null) {
                return Collections.emptyList();
            }
        }
        return asyncIterator.getPartitionMetrics();
    }

    @Override
    public List<DetailedMetrics> getShardMetrics() {
        synchronized (lock) {
            if (asyncIterator == null) {
                return Collections.emptyList();
            }
        }
        return asyncIterator.getShardMetrics();
    }

    /* -- Implement IterationHandleNotifier -- */

    @Override
    public void notifyNext() {
        if (Thread.holdsLock(lock)) {
            throw new IllegalStateException(
                "Already holding lock in call to notifyNext");
        }
        synchronized (lock) {

            /* Note new notify so other notify thread checks again */
            if (notifyingHandler) {
                newNotify = true;
                logger.finest("notifyNext newNotify=true");
                return;
            }

            /* Handler isn't initialized */
            if (nextHandler == null) {
                return;
            }

            /* Handler was already notified that the iteration is done */
            if (calledOnComplete) {
                return;
            }

            /*
             * Mark that we are delivering notifications and clear newNotify so
             * that we notice newer changes
             */
            notifyingHandler = true;
            newNotify = false;
            logger.finest("notifyNext");
        }

        Throwable exception = null;
        try {

            /*
             * Return local results to the app until:
             * - There are no more local results
             * - We have returned all the results requested by the app
             * - The iteration was canceled by the app
             * - The iteration was closed due to an error
             */
            while (true) {
                if (notifyOneNext()) {
                    break;
                }
            }
        } catch (RuntimeException e) {
            exception = e;
            throw e;
        } catch (Error e) {
            exception = e;
            throw e;
        } finally {

            /*
             * Make certain that notifyingHandler is cleared if there is an
             * unexpected exception -- it will have already been cleared
             * otherwise
             */
            if (exception != null) {
                synchronized (lock) {
                    notifyingHandler = false;
                }
                logger.log(Level.WARNING, "Unexpected exception: " + exception,
                           exception);
            }
        }
    }

    /**
     * Check for a single next element or for close, returning true if
     * notifications are done because there are no more elements or the
     * iterator is closed.  This method is called after setting
     * notifyingHandler with the lock held, so it can read most fields without
     * synchronization, but still needs to synchronize for updates.
     */
    private boolean notifyOneNext() {

        /* Get next element and closed status */
        E next = null;
        Throwable closeException = null;

        if (cancelCalled) {
            asyncIterator.close();
        } else if (requests > 0 && !asyncIterator.isClosed()) {
            /*
             * The isClosed() call above is to cover the case where the iterator
             * was closed due to an exception thrown by the remote request.
             */
            try {
                next = asyncIterator.nextLocal();
            } catch (Throwable e) {
                onNext(null, e);
                closeException = e;
            }
        }
        if (next != null) {
            closeException = onNext(next, null);

            /* Delivering next failed: terminate the iteration */
            if (closeException != null) {
                asyncIterator.close();
                next = null;
            }
        }
        final boolean isClosed =
            (closeException != null) ? true :
            (next != null) ? false :
            asyncIterator.isClosed();

        final long originalRequests = requests;
        final boolean done;
        synchronized (lock) {

            /* Decrement requests if we delivered a next element */
            if (next != null) {
                assert requests > 0;
                requests--;
            }
            if (isClosed) {

                /* Iteration is complete */
                assert !calledOnComplete;
                calledOnComplete = true;
                notifyingHandler = false;
                done = true;
            } else if (newNotify) {

                /* Clear and try again */
                newNotify = false;
                done = false;
            } else if (next == null) {

                /* No next and no new notifications, so we're done */
                notifyingHandler = false;
                done = true;
            } else {

                /* Check for more elements */
                done = false;
            }
        }
        if (logger.isLoggable(Level.FINEST)) {
            logger.finest("notifyNext next=" + next +
                          " isClosed=" + isClosed +
                          " newNotify=" + newNotify +
                          " originalRequests=" + originalRequests +
                          (closeException != null ?
                           " closeException=" + closeException :
                           "") +
                          " done=" + done);
        }
        if (isClosed) {
            if (closeException == null) {
                closeException = asyncIterator.getCloseException();
            }
            onComplete(closeException);
        }
        return done;
    }

    /**
     * Deliver a next iteration result, returning any exception thrown during
     * delivery, or null if the call completes normally.
     */
    private Throwable onNext(E next, Throwable exception) {
        try {
            nextHandler.onResult(next, exception);
            return null;
        } catch (Throwable t) {
            if (logger.isLoggable(Level.FINEST)) {
                if (exception == null) {
                    logger.finest(
                        "Problem delivering result to next handler: " +
                        nextHandler +
                        " result: " + next +
                        " exception from handler: " + t);
                } else {
                    logger.finest(
                        "Problem delivering exception to next handler: " +
                        nextHandler +
                        " exception being delivered: " + exception +
                        " exception from handler: " + t);
                }
            }
            return t;
        }
    }

    /**
     * Deliver the completion result.
     */
    private void onComplete(Throwable exception) {
        try {
            completionHandler.onComplete(exception);
        } catch (Throwable t) {
            logger.warning(
                "Problem notifying completion handler: " + completionHandler +
                (exception != null ?
                 " exception being delivered: " + exception :
                 "") +
                " exception from handler: " + t);
        }
    }

    /**
     * Close the iteration if the handle is garbage collected without being
     * closed.
     */
    @Override
    protected void finalize() {
        cancel();
    }
}
