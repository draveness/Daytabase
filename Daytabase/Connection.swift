//
//  Connection.swift
//  Database
//
//  Created by Draveness on 01/03/2017.
//  Copyright © 2017 Draveness. All rights reserved.
//

import Foundation
import CSQLite3

public enum DatabasePolicy: Int {
    case containment
    case share
    case copy
}

public final class Connection: NSObject {
    let database: Database
    var db: OpaquePointer?

    let connectionQueue: DispatchQueue = DispatchQueue(label: "daytabase.connection.queue")
    let isOnConnectionQueueKey = DispatchSpecificKey<Bool>()

    let objectCache: Cache = Cache(capacity: 100)


    init(database: Database) {
        self.database = database

        super.init()

        connectionQueue.setSpecific(key: isOnConnectionQueueKey, value: false)

        let flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_NOMUTEX | SQLITE_OPEN_PRIVATECACHE
        let status = sqlite3_open_v2(database.databasePath, &db, flags, nil)

        if status != SQLITE_OK {
            if let _ = db {
                Daytabase.log.error("Error opening database: \(status) \(daytabase_errmsg(self.db))")
            } else {
                Daytabase.log.error("Error opening database: \(status)")
            }
        }

    }

    var snapshot: Int64 = 0

    var longLivedReadTransaction: ReadTransaction?

    var lock: os_unfair_lock = os_unfair_lock()
    var writeQueueSuspended: Bool = false
    var activeReadWriteTransaction: Bool = false

    func prepare() {
        snapshot = database.snapshot
    }

    func initiailzeObjectCache() {
    }

    // MARK: - Transactions

    public func read(block: (ReadTransaction) -> Void) {
        connectionQueue.sync {
            if let transaction = longLivedReadTransaction {
                block(transaction)
            } else {
                let transation = self.newReadTransaction()
                self.preRead(transaction: transation)
                block(transation)
                self.postRead(transaction: transation)
            }
        }
    }

    public func readWrite(block: (ReadWriteTransaction) -> Void) {
        connectionQueue.sync {
            if let _ = longLivedReadTransaction {
                Daytabase.log.warning("Implicitly ending long-lived read transaction on connection \(self), database \(self.database)")
                _ = endLongLivedTransaction()
            }

            self.preWriteQueue()
            let transation = self.newReadWriteTransaction()
            self.preReadWrite(transaction: transation)
            block(transation)
            self.postReadWrite(transaction: transation)
            self.postWriteQueue()
        }
    }

    public func asyncRead(block: @escaping (ReadTransaction) -> Void) {
        asyncRead(block: block, completion: nil)
    }

    public func asyncRead(block: @escaping (ReadTransaction) -> Void,
                          completionQueue: DispatchQueue = DispatchQueue.main,
                          completion: ((Void) -> Void)? = nil) {
        connectionQueue.async {
            if let transaction = self.longLivedReadTransaction {
                block(transaction)
            } else {
                let transaction = self.newReadTransaction()
                self.preRead(transaction: transaction)
                block(transaction)
                self.postRead(transaction: transaction)
            }

            if let completion = completion {
                completionQueue.async(execute: completion)
            }
        }
    }

    public func asyncReadWrite(block: @escaping (ReadWriteTransaction) -> Void) {
        asyncReadWrite(block: block, completion: nil)
    }

    public func asyncReadWrite(block: @escaping (ReadWriteTransaction) -> Void,
                               completionQueue: DispatchQueue = DispatchQueue.main,
                               completion: ((Void) -> Void)? = nil) {
        connectionQueue.async {
            if let _ = self.longLivedReadTransaction {
                Daytabase.log.warning("Implicitly ending long-lived read transaction on connection \(self), database \(self.database)")
                _ = self.endLongLivedTransaction()
            } else {
                let transaction = self.newReadWriteTransaction()
                self.preReadWrite(transaction: transaction)
                block(transaction)
                self.postReadWrite(transaction: transaction)
            }

            if let completion = completion {
                completionQueue.async(execute: completion)
            }
        }
    }

    public func flushTransaction(completionQueue: DispatchQueue = DispatchQueue.main,
                                 completion: @escaping (Void) -> Void) {
        connectionQueue.async(execute: completion)
    }

    // MARK: - Transaction States

    func newReadTransaction() -> ReadTransaction {
        return ReadTransaction(connection: self)
    }

    func newReadWriteTransaction() -> ReadWriteTransaction {
        return ReadWriteTransaction(connection: self, readWrite: true)
    }

    func preRead(transaction: ReadTransaction) {
        transaction.begin()
    }

    func postRead(transaction: ReadTransaction) {
        transaction.commit()
    }

    func preReadWrite(transaction: ReadWriteTransaction) {
        transaction.begin()
    }

    func postReadWrite(transaction: ReadWriteTransaction) {
        transaction.commit()
    }

    // MARK: - Long Lived Transaction

    func endLongLivedTransaction() -> [Notification] {
        return []
    }

    func preWriteQueue() {
        os_unfair_lock_lock(&lock)
        if writeQueueSuspended {
            self.database.writeQueue.resume()
            writeQueueSuspended = false
        }
        activeReadWriteTransaction = true
        os_unfair_lock_unlock(&lock)
    }

    func postWriteQueue() {
        os_unfair_lock_lock(&lock)
        activeReadWriteTransaction = false
        os_unfair_lock_unlock(&lock)
    }
}

