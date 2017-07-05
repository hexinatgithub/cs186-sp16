import logging

from kvstore import DBMStore, InMemoryKVStore

LOG_LEVEL = logging.WARNING

KVSTORE_CLASS = InMemoryKVStore

"""
Possible abort modes.
"""
USER = 0
DEADLOCK = 1

class RequestLock:
    """
    The item in the Lock's request lock queue.
    """
    def __init__(self, transaction, mode):
        """
        @param xid: transaction.
        @param mode: request lock mode.
        """
        self.transaction = transaction
        self.mode = mode

class Lock:
    """
    The lock infomation about the key in lock table.
    """
    ExclusiveLock = "ExclusiveLock"
    SharedLock = "SharedLock"
    
    def __init__(self, transaction, mode, request_queue=None):
        self.transactions = [transaction]
        self.mode = mode
        self.request_queue = request_queue
        
    def request_lock(self, transaction, mode):
        """
        Return True if request is grant,
        else append the request in the request queue and return False.
        """
        if self.can_acquire_lock(transaction, mode):
            if transaction not in self.transactions:
                self.transactions.append(transaction)
            self.mode = mode
            return True
        else:
            if transaction in self.transactions:
                self.transactions.remove(transaction)
            self._append_request(transaction, mode)
            return False
    
    def first_current_transaction(self):
        if len(self.current_transactions()):
            return self.current_transactions()[0]
        return None
    
    def current_transactions(self):
        return self.transactions
    
    def hold_lock(self, transaction, mode):
        """
        Return True if transaction current hold the lock and match mode.
        """
        return transaction in self.transactions and self.mode is mode

    def can_acquire_lock(self, transaction, mode):
        """
        Return True if can grant transaction's lock else False.
        """
        if len(self.transactions) == 0:
            return True
        elif self.mode is Lock.ExclusiveLock:
            return transaction in self.transactions
        elif mode is Lock.SharedLock:
            return True
        elif len(self.transactions) == 1 and transaction in self.transactions:
            return mode is Lock.ExclusiveLock
        return False
    
    def release_lock(self, transaction):
        """
        Release the current lock hold by transation.
        If there is a queue not empty, grant the next transation request.
        """
        if transaction in self.transactions:
            self.transactions.remove(transaction)
            
        if len(self.transactions) == 0:
            self.mode = None
            self._grant_request()
    
    def _append_request(self, transaction, mode):
        """
        Append the lock in the request queue.
        If transaction already request the lock before, upgrade it.
        """
        def has_request_and_update():
            for r in self.request_queue:
                if r.transaction is transaction:
                    if r.mode is Lock.SharedLock:
                        r.mode = mode
                        return True
            return False

        if self.request_queue:
            if not has_request_and_update():
                self.request_queue.append(RequestLock(transaction, mode))
        else:
            self.request_queue = [RequestLock(transaction, mode)]
    
    def _grant_request(self):
        """
        Grant next transaction request in the request queue.
        """
        if self.request_queue and len(self.request_queue):
            request = self.request_queue.pop(0)
            self.transactions.append(request.transaction)
            self.mode = request.mode
            

"""
Part I: Implementing request handling methods for the transaction handler

The transaction handler has access to the following objects:

self._lock_table: the global lock table. More information in the README.

self._acquired_locks: a list of locks acquired by the transaction. Used to
release locks when the transaction commits or aborts. This list is initially
empty.

self._desired_lock: the lock that the transaction is waiting to acquire as well
as the operation to perform. This is initialized to None.

self._xid: this transaction's ID. You may assume each transaction is assigned a
unique transaction ID.

self._store: the in-memory key-value store. You may refer to kvstore.py for
methods supported by the store.

self._undo_log: a list of undo operations to be performed when the transaction
is aborted. The undo operation is a tuple of the form (@key, @value). This list
is initially empty.

You may assume that the key/value inputs to these methods are already type-
checked and are valid.
"""

class TransactionHandler:

    def __init__(self, lock_table, xid, store):
        self._lock_table = lock_table
        self._acquired_locks = []
        self._desired_lock = None
        self._xid = xid
        self._store = store
        self._undo_log = []

    def perform_put(self, key, value):
        """
        Handles the PUT request. You should first implement the logic for
        acquiring the exclusive lock. If the transaction can successfully
        acquire the lock associated with the key, insert the key-value pair
        into the store.

        Hint: if the lock table does not contain the key entry yet, you should
        create one.
        Hint: be aware that lock upgrade may happen.
        Hint: remember to update self._undo_log so that we can undo all the
        changes if the transaction later gets aborted. See the code in abort()
        for the exact format.

        @param self: the transaction handler.
        @param key, value: the key-value pair to be inserted into the store.

        @return: if the transaction successfully acquires the lock and performs
        the insertion/update, returns 'Success'. If the transaction cannot
        acquire the lock, returns None, and saves the lock that the transaction
        is waiting to acquire in self._desired_lock.
        """
        # Part 1.1: your code here!
        lock = self._lock_table.get(key)
        if lock is None:
            lock = self._lock_table[key] = Lock(self, Lock.ExclusiveLock)
        if lock.request_lock(self, Lock.ExclusiveLock):
            old_value = self._store.get(key)
            self._undo_log.append((key, old_value))
            self._acquired_locks.append(key)
            self._store.put(key, value)
            return 'Success'
        else:
            op = lambda: self.perform_put(key, value)
            self._desired_lock = (key, Lock.ExclusiveLock, op)
            return None

    def perform_get(self, key):
        """
        Handles the GET request. You should first implement the logic for
        acquiring the shared lock. If the transaction can successfully acquire
        the lock associated with the key, read the value from the store.

        Hint: if the lock table does not contain the key entry yet, you should
        create one.

        @param self: the transaction handler.
        @param key: the key to look up from the store.

        @return: if the transaction successfully acquires the lock and reads
        the value, returns the value. If the key does not exist, returns 'No
        such key'. If the transaction cannot acquire the lock, returns None,
        and saves the lock that the transaction is waiting to acquire in
        self._desired_lock.
        """
        # Part 1.1: your code here!
        lock = self._lock_table.get(key)
        if lock is None:
            lock = self._lock_table[key] = Lock(self, Lock.SharedLock)
        if lock.request_lock(self, Lock.SharedLock):
            self._acquired_locks.append(key)
            value = self._store.get(key)
            if value is None:
                return 'No such key'
            else:
                return value
        else:
            op = lambda: self.perform_get(key)
            self._desired_lock = (key, Lock.SharedLock, op)
            return None

    def release_and_grant_locks(self):
        """
        Releases all locks acquired by the transaction and grants them to the
        next transactions in the queue. This is a helper method that is called
        during transaction commits or aborts. 

        Hint: you can use self._acquired_locks to get a list of locks acquired
        by the transaction.
        Hint: be aware that lock upgrade may happen.

        @param self: the transaction handler.
        """
        for l in self._acquired_locks:
            # Part 1.2: your code here!
            lock = self._lock_table.get(l)
            lock.release_lock(self)
        self._acquired_locks = []

    def commit(self):
        """
        Commits the transaction.

        Note: This method is already implemented for you, and you only need to
        implement the subroutine release_locks().

        @param self: the transaction handler.

        @return: returns 'Transaction Completed'
        """
        self.release_and_grant_locks()
        return 'Transaction Completed'

    def abort(self, mode):
        """
        Aborts the transaction.

        Note: This method is already implemented for you, and you only need to
        implement the subroutine release_locks().

        @param self: the transaction handler.
        @param mode: mode can either be USER or DEADLOCK. If mode == USER, then
        it means that the abort is issued by the transaction itself (user
        abort). If mode == DEADLOCK, then it means that the transaction is
        aborted by the coordinator due to deadlock (deadlock abort).

        @return: if mode == USER, returns 'User Abort'. If mode == DEADLOCK,
        returns 'Deadlock Abort'.
        """
        while (len(self._undo_log) > 0):
            k,v = self._undo_log.pop()
            self._store.put(k, v)
        self.release_and_grant_locks()
        if (mode == USER):
            return 'User Abort'
        else:
            return 'Deadlock Abort'

    def check_lock(self):
        """
        If perform_get() or perform_put() returns None, then the transaction is
        waiting to acquire a lock. This method is called periodically to check
        if the lock has been granted due to commit or abort of other
        transactions. If so, then this method returns the string that would 
        have been returned by perform_get() or perform_put() if the method had
        not been blocked. Otherwise, this method returns None.

        As an example, suppose Joe is trying to perform 'GET a'. If Nisha has an
        exclusive lock on key 'a', then Joe's transaction is blocked, and
        perform_get() returns None. Joe's server handler starts calling
        check_lock(), which keeps returning None. While this is happening, Joe
        waits patiently for the server to return a response. Eventually, Nisha
        decides to commit his transaction, releasing his exclusive lock on 'a'.
        Now, when Joe's server handler calls check_lock(), the transaction
        checks to make sure that the lock has been acquired and returns the
        value of 'a'. The server handler then sends the value back to Joe.

        Hint: self._desired_lock contains the lock that the transaction is
        waiting to acquire.
        Hint: remember to update the self._acquired_locks list if the lock has
        been granted.
        Hint: if the transaction has been granted an exclusive lock due to lock
        upgrade, remember to clean up the self._acquired_locks list.
        Hint: remember to update self._undo_log so that we can undo all the
        changes if the transaction later gets aborted.

        @param self: the transaction handler.

        @return: if the lock has been granted, then returns whatever would be
        returned by perform_get() and perform_put() when the transaction
        successfully acquired the lock. If the lock has not been granted,
        returns None.
        """
        # Part 1.3: your code here!
        if not self._desired_lock:
            return None
        
        key, lock_mode, op = self._desired_lock
        lock = self._lock_table.get(key)
        if lock.hold_lock(self, lock_mode):
            v = op()
            if v:
                self._desired_lock = None
            return v
        return None




"""
Part II: Implement deadlock detection method for the transaction coordinator

The transaction coordinator has access to the following object:

self._lock_table: see description from Part I
"""

class TransactionCoordinator:

    def __init__(self, lock_table):
        self._lock_table = lock_table

    def detect_deadlocks(self):
        """
        Constructs a waits-for graph from the lock table, and runs a cycle
        detection algorithm to determine if a transaction needs to be aborted.
        You may choose which one transaction you plan to abort, as long as your
        choice is deterministic. For example, if transactions 1 and 2 form a
        cycle, you cannot return transaction 1 sometimes and transaction 2 the
        other times.

        This method is called periodically to check if any operations of any
        two transactions conflict. If this is true, the transactions are in
        deadlock - neither can proceed. If there are multiple cycles of
        deadlocked transactions, then this method will be called multiple
        times, with each call breaking one of the cycles, until it returns None
        to indicate that there are no more cycles. Afterward, the surviving
        transactions will continue to run as normal.

        Note: in this method, you only need to find and return the xid of a
        transaction that needs to be aborted. You do not have to perform the
        actual abort.

        @param self: the transaction coordinator.

        @return: If there are no cycles in the waits-for graph, returns None.
        Otherwise, returns the xid of a transaction in a cycle.
        """
        def get_desired_key_tranc(lock):
            t = lock.first_current_transaction()
            t_desired_lock = t._desired_lock
            return t_desired_lock[0], t

        for key, lock in self._lock_table.items():
            try:
                desired_key, t = get_desired_key_tranc(lock)
                other_lock = self._lock_table[desired_key]
                other_desired_key, other_t = get_desired_key_tranc(other_lock)
                if other_desired_key == key:
                    return min(t._xid, other_t._xid)
            except Exception:
                continue
