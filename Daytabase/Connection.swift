//
//  Connection.swift
//  Database
//
//  Created by Draveness on 01/03/2017.
//  Copyright © 2017 Draveness. All rights reserved.
//

import Foundation
import CSQLite3

public final class Connection {
    let database: Database
    var db: OpaquePointer?

    let connectionQueue: DispatchQueue = DispatchQueue(label: "daytabase.connection.queue")
    let isOnConnectionQueueKey = DispatchSpecificKey<Bool>()

    let objectCache: Cache = Cache(capacity: 100)


    init(database: Database) {
        self.database = database

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

    var lock: OSSpinLock = OS_SPINLOCK_INIT
    var writeQueueSuspended: Bool = false
    var activeReadWriteTransaction: Bool = false

    func prepare() {
        snapshot = database.snapshot
    }

    func initiailzeObjectCache() {
    }

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
        asyncRead(block: block, completionBlock: nil)
    }

    public func asyncRead(block: @escaping (ReadTransaction) -> Void,
                          completionQueue: DispatchQueue = DispatchQueue.main,
                          completionBlock: ((Void) -> Void)? = nil) {
        connectionQueue.async {
            if let transaction = self.longLivedReadTransaction {
                block(transaction)
            } else {
                let transaction = self.newReadTransaction()
                self.preRead(transaction: transaction)
                block(transaction)
                self.postRead(transaction: transaction)
            }

            if let completionBlock = completionBlock {
                completionQueue.async(execute: completionBlock)
            }
        }
    }

    public func asyncReadWrite(block: @escaping (ReadWriteTransaction) -> Void,
                               completionQueue: DispatchQueue = DispatchQueue.main,
                               completionBlock: ((Void) -> Void)? = nil){
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

            if let completionBlock = completionBlock {
                completionQueue.async(execute: completionBlock)
            }
        }
    }

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
}

