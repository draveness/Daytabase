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

    var beginImmediateTransactionStatement: OpaquePointer? {
        get {
            var statement: OpaquePointer?
            let sql = "BEGIN IMMEDIATE TRANSACTION;"
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

    var getCollectionCountStatement: OpaquePointer? {
        get {
            var statement: OpaquePointer?
            let sql = "SELECT COUNT(DISTINCT collection) AS NumberOfRows FROM \"\(defaultTableName)\";"
            prepareSQL(sql, statement: &statement, name: #function, in: db)
            return statement
        }
    }

    var getKeyCountForCollectionStatement: OpaquePointer? {
        get {
            var statement: OpaquePointer?
            let sql = "SELECT COUNT(*) AS NumberOfRows FROM \"\(defaultTableName)\" WHERE \"collection\" = ?;"
            prepareSQL(sql, statement: &statement, name: #function, in: db)
            return statement
        }
    }

    var getKeyCountForAllStatement: OpaquePointer? {
        get {
            var statement: OpaquePointer?
            let sql = "SELECT COUNT(*) AS NumberOfRows FROM \"\(defaultTableName)\";"
            prepareSQL(sql, statement: &statement, name: #function, in: db)
            return statement
        }
    }

    var getCountForRowidStatement: OpaquePointer? {
        get {
            var statement: OpaquePointer?
            let sql = "SELECT COUNT(*) AS NumberOfRows FROM \"\(defaultTableName)\" WHERE \"rowid\" = ?;"
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

    var getKeyForRowidStatement: OpaquePointer? {
        get {
            var statement: OpaquePointer?
            let sql = "SELECT \"collection\", \"key\" FROM \"\(defaultTableName)\" WHERE \"rowid\" = ?;"
            prepareSQL(sql, statement: &statement, name: #function, in: db)
            return statement
        }
    }

    var getDataForRowidStatement: OpaquePointer? {
        get {
            var statement: OpaquePointer?
            let sql = "SELECT \"data\" FROM \"\(defaultTableName)\" WHERE \"rowid\" = ?;"
            prepareSQL(sql, statement: &statement, name: #function, in: db)
            return statement
        }
    }

    var getMetadataForRowidStatement: OpaquePointer? {
        get {
            var statement: OpaquePointer?
            let sql = "SELECT \"metadata\" FROM \"\(defaultTableName)\" WHERE \"rowid\" = ?;"
            prepareSQL(sql, statement: &statement, name: #function, in: db)
            return statement
        }
    }

    var getAllForRowidStatement: OpaquePointer? {
        get {
            var statement: OpaquePointer?
            let sql = "SELECT \"data\", \"metadata\" FROM \"\(defaultTableName)\" WHERE \"rowid\" = ?;"
            prepareSQL(sql, statement: &statement, name: #function, in: db)
            return statement
        }
    }

    var getDataForKeyStatement: OpaquePointer? {
        get {
            var statement: OpaquePointer?
            let sql = "SELECT \"rowid\", \"data\" FROM \"\(defaultTableName)\" WHERE \"collection\" = ? AND \"key\" = ?;"
            prepareSQL(sql, statement: &statement, name: #function, in: db)
            return statement
        }
    }

    var getMetadataForKeyStatement: OpaquePointer? {
        get {
            var statement: OpaquePointer?
            let sql = "SELECT \"rowid\", \"metadata\" FROM \"\(defaultTableName)\" WHERE \"collection\" = ? AND \"key\" = ?;"
            prepareSQL(sql, statement: &statement, name: #function, in: db)
            return statement
        }
    }

    var getAllForKeyStatement: OpaquePointer? {
        get {
            var statement: OpaquePointer?
            let sql = "SELECT \"rowid\", \"data\", \"metadata\" FROM \"\(defaultTableName)\";"
            prepareSQL(sql, statement: &statement, name: #function, in: db)
            return statement
        }
    }

    var insertForRowidStatement: OpaquePointer? {
        get {
            var statement: OpaquePointer?
            let sql = "INSERT INTO \"\(defaultTableName)\""
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

    var updateObjectForRowidStatement: OpaquePointer? {
        get {
            var statement: OpaquePointer?
            let sql = "UPDATE \"\(defaultTableName)\" SET \"data\" = ? WHERE \"rowid\" = ?;"
            prepareSQL(sql, statement: &statement, name: #function, in: db)
            return statement
        }
    }

    var updateMetadataForRowidStatement: OpaquePointer? {
        get {
            var statement: OpaquePointer?
            let sql = "UPDATE \"\(defaultTableName)\" SET \"metadata\" = ? WHERE \"rowid\" = ?;"
            prepareSQL(sql, statement: &statement, name: #function, in: db)
            return statement
        }
    }

    var removeForRowidStatement: OpaquePointer? {
        get {
            var statement: OpaquePointer?
            let sql = "DELETE FROM \"\(defaultTableName)\" WHERE \"rowid\" = ?;"
            prepareSQL(sql, statement: &statement, name: #function, in: db)
            return statement
        }
    }

    var removeCollectionStatement: OpaquePointer? {
        get {
            var statement: OpaquePointer?
            let sql = "DELETE FROM \"\(defaultTableName)\" WHERE \"collection\" = ?;"
            prepareSQL(sql, statement: &statement, name: #function, in: db)
            return statement
        }
    }

    var removeAllStatement: OpaquePointer? {
        get {
            var statement: OpaquePointer?
            let sql = "DELETE FROM \"\(defaultTableName)\";"
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

    var yapGetDataForKeyStatement: OpaquePointer? {
        get {
            var statement: OpaquePointer?
            let sql = "SELECT \"data\" FROM \"\(extensionTableName)\" WHERE \"extension\" = ? AND \"key\" = ?;"
            prepareSQL(sql, statement: &statement, name: #function, in: db)
            return statement
        }
    }

    var yapSetDataForKeyStatement: OpaquePointer? {
        get {
            var statement: OpaquePointer?
            let sql = "INSERT OR REPLACE INTO \"\(extensionTableName)\" (\"extension\", \"key\", \"data\") VALUES (?, ?, ?);"
            prepareSQL(sql, statement: &statement, name: #function, in: db)
            return statement
        }
    }

    var yapRemoveForKeyStatement: OpaquePointer? {
        get {
            var statement: OpaquePointer?
            let sql = "DELETE FROM \"\(extensionTableName)\" WHERE \"extension\" = ? AND \"key\" = ?;"
            prepareSQL(sql, statement: &statement, name: #function, in: db)
            return statement
        }
    }

    var yapRemoveExtensionStatement: OpaquePointer? {
        get {
            var statement: OpaquePointer?
            let sql = "DELETE FROM \"\(extensionTableName)\" WHERE \"extension\" = ?;"
            prepareSQL(sql, statement: &statement, name: #function, in: db)
            return statement
        }
    }
}
