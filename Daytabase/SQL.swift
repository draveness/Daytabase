//
//  SQL.swift
//  Daytabase
//
//  Created by Draveness on 3/3/17.
//  Copyright © 2017 Draveness. All rights reserved.
//

import Foundation
import CSQLite3

fileprivate func prepareSQL(_ sql: String, statement: UnsafeMutablePointer<OpaquePointer?>, name: String, in db: OpaquePointer?) {
    let status = sqlite3_prepare_v2(db, sql, sql.characters.count + 1, statement, nil)
    if status != SQLITE_OK {
        Daytabase.log.error("Error creating \(name): \(status) \(daytabase_errmsg(db))")
    }
}

extension Database {
    var beginTransactionStatement: OpaquePointer? {
        get {
            var statement: OpaquePointer?
            let sql = "BEGIN TRANSACTION;"
            prepareSQL(sql, statement: &statement, name: #function, in: db)
            return statement
        }
    }

    var commitTransactionStatement: OpaquePointer? {
        get {
            var statement: OpaquePointer?
            let sql = "COMMIT TRANSACTION;"
            prepareSQL(sql, statement: &statement, name: #function, in: db)
            return statement
        }
    }

    var rollbackTransactionStatement: OpaquePointer? {
        get {
            var statement: OpaquePointer?
            let sql = "ROLLBACK TRANSACTION;"
            prepareSQL(sql, statement: &statement, name: #function, in: db)
            return statement
        }
    }


    static func getSqliteVersionStatement(with database: OpaquePointer?) -> OpaquePointer? {
        var statement: OpaquePointer?
        let sql = "SELECT sqlite_version();"
        prepareSQL(sql, statement: &statement, name: #function, in: database)
        return statement
    }

    // MARK: - For Default Table

    var getDataForKeyStatement: OpaquePointer? {
        get {
            var statement: OpaquePointer?
            let sql = "SELECT \"rowid\", \"data\" FROM \"\(defaultTableName)\" WHERE \"collection\" = ? AND \"key\" = ?;"
            prepareSQL(sql, statement: &statement, name: #function, in: db)
            return statement
        }
    }

    var insertForRowidStatement: OpaquePointer? {
        get {
            var statement: OpaquePointer?
            let sql =
                "INSERT INTO \"\(defaultTableName)\"" +
            " (\"collection\", \"key\", \"data\", \"metadata\") VALUES (?, ?, ?, ?);"
            prepareSQL(sql, statement: &statement, name: #function, in: db)
            return statement
        }
    }

    var updateAllForRowidStatement: OpaquePointer? {
        get {
            var statement: OpaquePointer?
            let sql = "UPDATE \"\(defaultTableName)\" SET \"data\" = ?, \"metadata\" = ? WHERE \"rowid\" = ?;"
            prepareSQL(sql, statement: &statement, name: #function, in: db)
            return statement
        }
    }

    var getRowidForKeyStatement: OpaquePointer? {
        get {
            var statement: OpaquePointer?
            let sql = "SELECT \"rowid\" FROM \"\(defaultTableName)\" WHERE \"collection\" = ? AND \"key\" = ?;"
            prepareSQL(sql, statement: &statement, name: #function, in: db)
            return statement
        }
    }

    // MARK: - For Extension Table

    var readSnapshotInExtensionStatement: OpaquePointer? {
        get {
            var statement: OpaquePointer?
            let sql = "SELECT \"data\" FROM \"\(extensionTableName)\" WHERE \"extension\" = ? AND \"key\" = ?;"
            prepareSQL(sql, statement: &statement, name: #function, in: db)
            return statement
        }
    }

    var writeSnapshotInExtensionStatement: OpaquePointer? {
        get {
            var statement: OpaquePointer?
            let sql = "INSERT OR REPLACE INTO \"\(extensionTableName)\" (\"extension\", \"key\", \"data\") VALUES (?, ?, ?);"
            prepareSQL(sql, statement: &statement, name: #function, in: db)
            return statement
        }
    }


}
