//
//  SQL.swift
//  Daytabase
//
//  Created by Draveness on 3/3/17.
//  Copyright © 2017 Draveness. All rights reserved.
//

import Foundation
import CSQLite3

extension Database {
    func prepareSQL(_ sql: String, statement: UnsafeMutablePointer<OpaquePointer?>, name: String) {
        let status = sqlite3_prepare_v2(db, sql, sql.characters.count + 1, statement, nil)
        if status != SQLITE_OK {
            Daytabase.log.error("Error creating \(name): \(status) \(daytabase_errmsg(self.db))")
        }
    }

    var beginTransactionStatement: OpaquePointer? {
        get {
            var statement: OpaquePointer?
            let sql = "BEGIN TRANSACTION;"
            self.prepareSQL(sql, statement: &statement, name: #function)
            return statement
        }
    }

    var commitTransactionStatement: OpaquePointer? {
        get {
            var statement: OpaquePointer?
            let sql = "COMMIT TRANSACTION;"
            self.prepareSQL(sql, statement: &statement, name: #function)
            return statement
        }
    }

    var getDataForKeyInDapStatement: OpaquePointer? {
        get {
            var statement: OpaquePointer?
            let sql = "SELECT \"data\" FROM \"\(extensionTableName)\" WHERE \"extension\" = ? AND \"key\" = ?;"
            self.prepareSQL(sql, statement: &statement, name: #function)
            return statement
        }
    }

    var getDataForKeyStatement: OpaquePointer? {
        get {
            var statement: OpaquePointer?
            let sql = "SELECT \"rowid\", \"data\" FROM \"\(defaultTableName)\" WHERE \"collection\" = ? AND \"key\" = ?;"
            self.prepareSQL(sql, statement: &statement, name: #function)
            return statement
        }
    }

    var insertForRowidStatement: OpaquePointer? {
        get {
            var statement: OpaquePointer?
            let sql =
                "INSERT INTO \"\(defaultTableName)\"" +
            " (\"collection\", \"key\", \"data\", \"metadata\") VALUES (?, ?, ?, ?);"
            self.prepareSQL(sql, statement: &statement, name: #function)
            return statement
        }
    }

    var updateAllForRowidStatement: OpaquePointer? {
        get {
            var statement: OpaquePointer?
            let sql = "UPDATE \"\(defaultTableName)\" SET \"data\" = ?, \"metadata\" = ? WHERE \"rowid\" = ?;"
            self.prepareSQL(sql, statement: &statement, name: #function)
            return statement
        }
    }

    var getRowidForKeyStatement: OpaquePointer? {
        get {
            var statement: OpaquePointer?
            let sql = "SELECT \"rowid\" FROM \"\(defaultTableName)\" WHERE \"collection\" = ? AND \"key\" = ?;"
            self.prepareSQL(sql, statement: &statement, name: #function)
            return statement
        }
    }
}
