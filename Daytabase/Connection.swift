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
                print("Error opening database: \(status) \(sqlite3_errmsg(db))")
            } else {
                print("Error opening database: \(status)")
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


    func newReadTransaction() -> ReadTransaction {
        return ReadTransaction(connection: self)
    }

    func preRead(transaction: ReadTransaction) {
        transaction.begin()
    }

    func postRead(transaction: ReadTransaction) {

    }
}
