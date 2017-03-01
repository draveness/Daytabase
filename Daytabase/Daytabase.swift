//
//  Daytabase.swift
//  Daytabase
//
//  Created by Draveness on 01/03/2017.
//  Copyright © 2017 Draveness. All rights reserved.
//

import Foundation
import CSQLite3

public typealias DaytabaseSerializer = (String, String, Any) -> Data
public typealias DaytabaseDeserializer = (String, String, Data) -> Any?

let defaultSerializer: DaytabaseSerializer = { (collection: String, key: String, value: Any) in
    return NSKeyedArchiver.archivedData(withRootObject: value)
}

let defaultDeserializer: DaytabaseDeserializer = { (collection: String, key: String, data: Data) in
    return NSKeyedUnarchiver.unarchiveObject(with: data)
}

public final class Daytabase {
    public let databasePath: String
    public let objectSerializer: DaytabaseSerializer
    public let objectDeserializer: DaytabaseDeserializer
    public let metadataSerializer: DaytabaseSerializer
    public let metadataDeserializer: DaytabaseDeserializer
    
    public var db: OpaquePointer?
    
    init(path: String,
         serializer: @escaping DaytabaseSerializer = defaultSerializer,
         deserializer: @escaping DaytabaseDeserializer = defaultDeserializer,
         metadataSerializer: @escaping DaytabaseSerializer = defaultSerializer,
         metadataDeserializer: @escaping DaytabaseDeserializer = defaultDeserializer) {
        self.databasePath = path
        self.objectSerializer = serializer
        self.objectDeserializer = deserializer
        self.metadataSerializer = metadataSerializer
        self.metadataDeserializer = metadataDeserializer
        
        let isNewDatabase = FileManager.default.fileExists(atPath: path)
        
        let openConfigCreate = { () -> Bool in 
            let result = !(self.openDatabase() && self.configureDatabase(isNewDatabase: isNewDatabase) && self.createTables())
            if let db = self.db, !result {
                sqlite3_close(db)
                self.db = nil
            }
            return result
        }
        let result = openConfigCreate()
        if !result {
            print("Error opening database")
            return nil
        }
    }
    
    func openDatabase() -> Bool {
        let flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_NOMUTEX | SQLITE_OPEN_PRIVATECACHE
        let status = sqlite3_open_v2(databasePath, &db, flags, nil)

        if status == SQLITE_OK { return true }
    
        if let _ = db {
            print("Error opening database: \(status) \(sqlite3_errmsg(db))")
        } else {
            print("Error opening database: \(status)")
        }
        return false

    }
    
    
    // TODO
    func configureDatabase(isNewDatabase: Bool) -> Bool {
        return false
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
            print("Failed creating 'yap2' table: \(sqlite3_errmsg(db))")
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
            print("Failed creating 'database2' table: \(sqlite3_errmsg(db))")
            return false
        }
      
        let createIndexStatement = "CREATE UNIQUE INDEX IF NOT EXISTS \"true_primary_key\" ON \"database2\" ( \"collection\", \"key\" );"
        if sqlite3_exec(db, createIndexStatement, nil, nil, nil) != SQLITE_OK {
            print("Failed creating index on 'database2' table: \(sqlite3_errmsg(db))")
            return false
        }
        return true
    }
    
}
