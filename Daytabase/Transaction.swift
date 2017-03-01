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
            print("Error creating \(name): \(status) \(daytabase_errmsg(db))")
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

    var getDataForKeyStatement: OpaquePointer? {
        get {
            var statement: OpaquePointer?
            let sql = "SELECT \"rowid\", \"data\" FROM \"database2\" WHERE \"collection\" = ? AND \"key\" = ?;"
            self.prepareSQL(sql, statement: &statement, name: #function)
            return statement
        }
    }

    var insertForRowidStatement: OpaquePointer? {
        get {
            var statement: OpaquePointer?
            let sql =
                "INSERT INTO \"database2\"" +
                " (\"collection\", \"key\", \"data\", \"metadata\") VALUES (?, ?, ?, ?);"
            self.prepareSQL(sql, statement: &statement, name: #function)
            return statement
        }
    }

    func begin() {
        guard let statement = beginTransactionStatement else { return }
        let status = sqlite3_step(statement)
        if status != SQLITE_DONE {
            print("Couldn't begin transaction: \(status) \(daytabase_errmsg(db))")
        }
        sqlite3_reset(statement)
    }

    func commit() {
        guard let statement = commitTransactionStatement else { return }
        let status = sqlite3_step(statement)
        if status != SQLITE_DONE {
            print("Couldn't commit transaction: \(status) \(daytabase_errmsg(db))")
        }
        sqlite3_reset(statement)
    }

    static let SQLITE_COLUMN_START: Int32 = 0
    static let SQLITE_BIND_START: Int32 = 1
    let column_idx_rowid = SQLITE_COLUMN_START
    let column_idx_data = SQLITE_COLUMN_START + 1

    let bind_idx_collection = SQLITE_BIND_START
    let bind_idx_key = SQLITE_BIND_START + 1
    let bind_idx_data = SQLITE_BIND_START + 2
    let bind_idx_metadata = SQLITE_BIND_START + 3

    let SQLITE_STATIC = unsafeBitCast(0, to: sqlite3_destructor_type.self)

    public func valueFor(key: String, inCollection collection: String = "") -> Any? {
        return objectFor(key: key, inCollection: collection)
    }

    public func objectFor(key: String, inCollection collection: String = "") -> Any? {
        guard let statement = getDataForKeyStatement else { return nil }

        defer {
            sqlite3_clear_bindings(statement)
            sqlite3_reset(statement)
        }

        sqlite3_bind_text(statement, bind_idx_collection, collection, Int32(collection.characters.count), SQLITE_STATIC)
        sqlite3_bind_text(statement, bind_idx_key, key, Int32(key.characters.count), SQLITE_STATIC)

        let status = sqlite3_step(statement)
        if status == SQLITE_ROW {
            let _ = sqlite3_column_int64(statement, column_idx_rowid)
            let blob = sqlite3_column_blob(statement, column_idx_data)
            let count = sqlite3_column_bytes(statement, column_idx_data)
            guard let bytes = blob else { return nil }
            let data = Data(bytes: bytes, count: Int(count))
            return connection.database.objectDeserializer(collection, key, data)
        } else if status == SQLITE_ERROR {
            print("Error executing 'getDataForKeyStatement': \(status) \(daytabase_errmsg(db)) key(\(key))")
        }
        return nil
    }

}

public final class ReadWriteTransaction: ReadTransaction {
    func insert(object: Any, key: String, inCollection collection: String = "") {
        guard let statement = insertForRowidStatement else { return }

        defer {
            sqlite3_clear_bindings(statement)
            sqlite3_reset(statement)
        }

        let serializedObject = connection.database.objectSerializer(collection, key, object) as NSData
        let serializedMetadata = NSData()

        sqlite3_bind_text(statement, bind_idx_collection, collection, Int32(collection.characters.count), SQLITE_STATIC)
        sqlite3_bind_text(statement, bind_idx_key, key, Int32(key.characters.count), SQLITE_STATIC)

        sqlite3_bind_blob(statement, bind_idx_data,
                          serializedObject.bytes, Int32(serializedObject.length), SQLITE_STATIC);

        sqlite3_bind_blob(statement, bind_idx_metadata,
                          serializedMetadata.bytes, Int32(serializedMetadata.length), SQLITE_STATIC);
        let status = sqlite3_step(statement);
        if status == SQLITE_DONE {
            let _ = sqlite3_last_insert_rowid(db);
        } else {
            print("Error executing 'insertForRowidStatement': \(status) \(daytabase_errmsg(db)) key(\(key))")
        }
    }

    public func set(value: Any, key: String, inCollection collection: String = "") {
        set(object: value, key: key, inCollection: collection)
    }

    public func set(object: Any, key: String, inCollection collection: String = "") {
        // update
        insert(object: object, key: key, inCollection: collection)
    }
}
