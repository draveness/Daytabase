//
//  Database.swift
//  Database
//
//  Created by Draveness on 01/03/2017.
//  Copyright © 2017 Draveness. All rights reserved.
//

import Foundation
import CSQLite3

public typealias DatabaseSerializer = (String, String, Any) -> Data
public typealias DatabaseDeserializer = (String, String, Data) -> Any?

let defaultSerializer: DatabaseSerializer = { (collection: String, key: String, value: Any) in
    return NSKeyedArchiver.archivedData(withRootObject: value)
}

let defaultDeserializer: DatabaseDeserializer = { (collection: String, key: String, data: Data) in
    return NSKeyedUnarchiver.unarchiveObject(with: data)
}

func daytabase_errmsg(_ db: OpaquePointer?) -> String {
    return String(cString: sqlite3_errmsg(db))
}

public final class Database {
    public let databasePath: String
    public let objectSerializer: DatabaseSerializer
    public let objectDeserializer: DatabaseDeserializer
    public let metadataSerializer: DatabaseSerializer
    public let metadataDeserializer: DatabaseDeserializer
    
    public var db: OpaquePointer?

    public convenience init() {
        let file = "database123"
        self.init(file: file)
    }

    public convenience init(file: String) {
        let path = NSSearchPathForDirectoriesInDomains(.documentDirectory, .userDomainMask, true).first!.appending("/\(file).sqlite")
        self.init(path: path)
    }
    
    public init(path: String,
                serializer: @escaping DatabaseSerializer = defaultSerializer,
                deserializer: @escaping DatabaseDeserializer = defaultDeserializer,
                metadataSerializer: @escaping DatabaseSerializer = defaultSerializer,
                metadataDeserializer: @escaping DatabaseDeserializer = defaultDeserializer) {
        self.databasePath = path
        self.objectSerializer = serializer
        self.objectDeserializer = deserializer
        self.metadataSerializer = metadataSerializer
        self.metadataDeserializer = metadataDeserializer
        
        let isNewDatabase = FileManager.default.fileExists(atPath: path)
        
        let openConfigCreate = { () -> Bool in 
            let result = self.openDatabase() && self.configureDatabase(isNewDatabase: isNewDatabase) && self.createTables()
            if let db = self.db, !result {
                sqlite3_close(db)
                self.db = nil
            }
            return result
        }
        let result = openConfigCreate()
        if !result {
            Daytabase.Log.error("Error opening database")
        }
    }
    
    func openDatabase() -> Bool {
        let flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_NOMUTEX | SQLITE_OPEN_PRIVATECACHE
        let status = sqlite3_open_v2(databasePath, &db, flags, nil)

        if status == SQLITE_OK { return true }
    
        if let _ = db {
            Daytabase.Log.error("Error opening database: \(status) \(daytabase_errmsg(self.db))")
        } else {
            Daytabase.Log.error("Error opening database: \(status)")
        }
        return false

    }
    
    
    // TODO
    func configureDatabase(isNewDatabase: Bool) -> Bool {
        return true
    }
    
    func createTables() -> Bool {
        let createYapTableStatement =
            "CREATE TABLE IF NOT EXISTS \"yap2\"" +
            " (\"extension\" CHAR NOT NULL, " +
            "  \"key\" CHAR NOT NULL, " +
            "  \"data\" BLOB, " +
            "  PRIMARY KEY (\"extension\", \"key\")" +
            " );"
        if sqlite3_exec(db, createYapTableStatement, nil, nil, nil) != SQLITE_OK {
            Daytabase.Log.error("Failed creating 'yap2' table: \(daytabase_errmsg(self.db))")
            return false
        }
        
        let createDatabaseTableStatement =
            "CREATE TABLE IF NOT EXISTS \"database2\"" +
            " (\"rowid\" INTEGER PRIMARY KEY," +
            "  \"collection\" CHAR NOT NULL," +
            "  \"key\" CHAR NOT NULL," +
            "  \"data\" BLOB," +
            "  \"metadata\" BLOB" +
            " );";
        if sqlite3_exec(db, createDatabaseTableStatement, nil, nil, nil) != SQLITE_OK {
            Daytabase.Log.error("Failed creating 'database2' table: \(daytabase_errmsg(self.db))")
            return false
        }
      
        let createIndexStatement = "CREATE UNIQUE INDEX IF NOT EXISTS \"true_primary_key\" ON \"database2\" ( \"collection\", \"key\" );"
        if sqlite3_exec(db, createIndexStatement, nil, nil, nil) != SQLITE_OK {
            Daytabase.Log.error("Failed creating index on 'database2' table: \(daytabase_errmsg(self.db))")
            return false
        }
        return true
    }

    public func newConnection() -> Connection {
        return Connection(database: self)
    }

}
