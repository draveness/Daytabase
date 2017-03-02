//
//  Connection.swift
//  Daytabase
//
//  Created by Draveness on 01/03/2017.
//  Copyright © 2017 Draveness. All rights reserved.
//

import Foundation
import CSQLite3

public final class Connection {
    public let database: Daytabase
    var objectCache: Cache = Cache(capacity: 20)

    let connectionQueue: DispatchQueue = DispatchQueue(label: "DaytabaseConnectionQueue")
    let isOnConnectionQueueKey = DispatchSpecificKey<Bool>()

    var db: OpaquePointer?

    init(database: Daytabase) {
        self.database = database

        connectionQueue.setSpecific(key: isOnConnectionQueueKey, value: false)

        let flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_NOMUTEX | SQLITE_OPEN_PRIVATECACHE
        let status = sqlite3_open_v2(database.databasePath, &db, flags, nil)

        if status != SQLITE_OK {
            if let _ = db {
                Log.error("Error opening database: \(status) \(daytabase_errmsg(self.db))")
            } else {
                Log.error("Error opening database: \(status)")
            }
        }
    }

    public func read(block: (ReadTransaction) -> Void) {
        connectionQueue.sync {
            let transation = self.newReadTransaction()
            self.preRead(transaction: transation)
            block(transation)
            self.postRead(transaction: transation)
        }
    }

    public func readWrite(block: (ReadWriteTransaction) -> Void) {
        connectionQueue.sync {
            let transation = self.newReadWriteTransaction()
            self.preReadWrite(transaction: transation)
            block(transation)
            self.postReadWrite(transaction: transation)
        }
    }

    func initiailzeObjectCache() {
//        objectCache = Cache(capacity: 20)
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
