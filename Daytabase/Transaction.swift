//
//  DayTransaction.swift
//  Daytabase
//
//  Created by Draveness on 3/1/17.
//  Copyright © 2017 Draveness. All rights reserved.
//

import Foundation
import CSQLite3

let SQLITE_COLUMN_START: Int32 = 0
let SQLITE_BIND_START: Int32 = 1

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
    
    var updateAllForRowidStatement: OpaquePointer? {
        get {
            var statement: OpaquePointer?
            let sql = "UPDATE \"database2\" SET \"data\" = ?, \"metadata\" = ? WHERE \"rowid\" = ?;"
            self.prepareSQL(sql, statement: &statement, name: #function)
            return statement
        }
    }
    
    var getRowidForKeyStatement: OpaquePointer? {
        get {
            var statement: OpaquePointer?
            let sql = "SELECT \"rowid\" FROM \"database2\" WHERE \"collection\" = ? AND \"key\" = ?;"
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
    
    let SQLITE_STATIC = unsafeBitCast(0, to: sqlite3_destructor_type.self)

    public func value(forKey key: String, inCollection collection: String = "") -> Any? {
        return object(forkey: key, inCollection: collection)
    }

    public func object(forkey key: String, inCollection collection: String = "") -> Any? {
        guard let statement = getDataForKeyStatement else { return nil }

        defer {
            sqlite3_clear_bindings(statement)
            sqlite3_reset(statement)
        }
        let column_idx_rowid = SQLITE_COLUMN_START
        let column_idx_data = SQLITE_COLUMN_START + 1
        
        let bind_idx_collection = SQLITE_BIND_START
        let bind_idx_key = SQLITE_BIND_START + 1
        
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
    func rowid(forKey key: String, inCollection collection: String) -> Int64 {
        guard let statement = getRowidForKeyStatement else { return 0 }
        let column_idx_result   = SQLITE_COLUMN_START;
        let bind_idx_collection = SQLITE_BIND_START + 0;
        let bind_idx_key        = SQLITE_BIND_START + 1;
        
        sqlite3_bind_text(statement, bind_idx_collection, collection, Int32(collection.characters.count), SQLITE_STATIC)
        sqlite3_bind_text(statement, bind_idx_key, key, Int32(key.characters.count),  SQLITE_STATIC)
        
        let status = sqlite3_step(statement)
        if status == SQLITE_ROW {
            return sqlite3_column_int64(statement, column_idx_result)
        } else if status == SQLITE_ERROR {
            print("Error executing 'getRowidForKeyStatement': \(status) \(daytabase_errmsg(db)) key(\(key))")
        }
        
        defer {
            sqlite3_clear_bindings(statement)
            sqlite3_reset(statement)
        }

        return 0
    }
    func insert(object: Any, forKey key: String, inCollection collection: String = "") {
        guard let statement = insertForRowidStatement else { return }

        defer {
            sqlite3_clear_bindings(statement)
            sqlite3_reset(statement)
        }

        let bind_idx_collection = SQLITE_BIND_START
        let bind_idx_key = SQLITE_BIND_START + 1
        let bind_idx_data = SQLITE_BIND_START + 2
        let bind_idx_metadata = SQLITE_BIND_START + 3
        
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
    
    func update(object: Any, forKey key: String, inCollection collection: String = "") {
        guard let statement = updateAllForRowidStatement else { return }
        
        defer {
            sqlite3_clear_bindings(statement)
            sqlite3_reset(statement)
        }
        
        let rowid = self.rowid(forKey: key, inCollection: collection)
        
        let bind_idx_data     = SQLITE_BIND_START + 0
        let bind_idx_metadata = SQLITE_BIND_START + 1
        let bind_idx_rowid    = SQLITE_BIND_START + 2
        
        let serializedObject = connection.database.objectSerializer(collection, key, object) as NSData
        let serializedMetadata = NSData()
        
        sqlite3_bind_blob(statement, bind_idx_data,
                          serializedObject.bytes, Int32(serializedObject.length), SQLITE_STATIC);
        sqlite3_bind_blob(statement, bind_idx_metadata,
                          serializedMetadata.bytes, Int32(serializedMetadata.length), SQLITE_STATIC);
        sqlite3_bind_int64(statement, bind_idx_rowid, sqlite3_int64(rowid));
        sqlite3_bind_blob(statement, bind_idx_data,
                          serializedObject.bytes, Int32(serializedObject.length), SQLITE_STATIC);
        sqlite3_bind_blob(statement, bind_idx_metadata,
                          serializedMetadata.bytes, Int32(serializedMetadata.length), SQLITE_STATIC);

        let status = sqlite3_step(statement);
        if status == SQLITE_DONE {
            let _ = sqlite3_last_insert_rowid(db);
        } else {
            print("Error executing 'updateAllForRowidStatement': \(status) \(daytabase_errmsg(db)) key(\(key))")
        }
    }


    public func set(value: Any, forKey key: String, inCollection collection: String = "") {
        set(object: value, forKey: key, inCollection: collection)
    }

    public func set(object inObject: Any, forKey key: String, inCollection collection: String = "") {
        // update
        if let _ = object(forkey: key, inCollection: collection) {
            update(object: inObject, forKey: key, inCollection: collection)
        } else {
            insert(object: inObject, forKey: key, inCollection: collection)
        }
    }
}
