//
//  DayTransaction.swift
//  Daytabase
//
//  Created by Draveness on 3/1/17.
//  Copyright © 2017 Draveness. All rights reserved.
//

import Foundation
import CSQLite3

public class ReadTransaction {
    public let connection: Connection
    public let db: OpaquePointer?
    let isReadWrite: Bool
    init(connection: Connection, readWrite: Bool = false) {
        self.connection = connection
        self.db = connection.db
        self.isReadWrite = readWrite
    }

    func prepareSQL(_ sql: String, statement: UnsafeMutablePointer<OpaquePointer?>, name: String) {
        let status = sqlite3_prepare_v2(db, sql, sql.characters.count + 1, statement, nil)
        if status != SQLITE_OK {
            print("Error creating \(name): \(status) \(sqlite3_errmsg(db))")
        }
    }

    var beginTransactionStatement: OpaquePointer? {
        get {
            if self.beginTransactionStatement == nil {
                let sql = "BEGIN TRANSACTION;"
                self.prepareSQL(sql, statement: &self.beginTransactionStatement, name: #function)
            }
            return self.beginTransactionStatement
        }
        set {
            self.beginTransactionStatement = newValue
        }
    }

    var commitTransactionStatement: OpaquePointer? {
        get {
            if self.commitTransactionStatement == nil {
                let sql = "COMMIT TRANSACTION;"
                self.prepareSQL(sql, statement: &self.commitTransactionStatement, name: #function)
            }
            return self.commitTransactionStatement
        }
        set {
            self.commitTransactionStatement = newValue
        }
    }

    var getDataForKeyStatement: OpaquePointer? {
        get {
            if self.getDataForKeyStatement == nil {
                let sql = "SELECT \"rowid\", \"data\" FROM \"database2\" WHERE \"collection\" = ? AND \"key\" = ?;"
                self.prepareSQL(sql, statement: &self.getDataForKeyStatement, name: #function)
            }
            return self.getDataForKeyStatement
        }
        set {
            self.getDataForKeyStatement = newValue
        }
    }

    func begin() {
        guard let statement = beginTransactionStatement else { return }
        let status = sqlite3_step(statement)
        if status != SQLITE_OK {
            print("Couldn't begin transaction: \(status) \(sqlite3_errmsg(db))")
        }
        sqlite3_reset(statement)
    }

    func commit() {
        guard let statement = commitTransactionStatement else { return }
        let status = sqlite3_step(statement)
        if status != SQLITE_OK {
            print("Couldn't commit transaction: \(status) \(sqlite3_errmsg(db))")
        }
        sqlite3_reset(statement)
    }

    func objectFor(key: String, collection: String = "") -> Any {

    }

}

public final class ReadWriteTransaction: ReadTransaction {

}
