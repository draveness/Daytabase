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

public final class Database {
    public let databasePath: String
    public let objectSerializer: DatabaseSerializer
    public let objectDeserializer: DatabaseDeserializer
    public let metadataSerializer: DatabaseSerializer
    public let metadataDeserializer: DatabaseDeserializer

    let internalQueue = DispatchQueue(label: "daytabase.internal.queue")
    let checkpointQueue = DispatchQueue(label: "daytabase.checkpoint.queue")
    let snapshotQueue = DispatchQueue(label: "daytabase.snapshot.queue")
    let writeQueue = DispatchQueue(label: "daytabase.write.queue")

    let isOnSnapshotQueueKey = DispatchSpecificKey<Bool>()
    let isOnWriteQueueKey = DispatchSpecificKey<Bool>()

    var sqliteVersion = "Unknown"
    var snapshot: Int64 = 0

    var connectionStates: [ConnectionState] = []
    private var _connectionDefaults: ConnectionDefaults = ConnectionDefaults()
    public var connectionDefaults: ConnectionDefaults {
        get {
            var defaults: ConnectionDefaults!
            internalQueue.sync {
                defaults = self._connectionDefaults
            }
            return defaults
        }
    }

    static let DEFAULT_MAX_CONNECTION_POOL_COUNT = 5    // connections
    static let DEFAULT_CONNECTION_POOL_LIFETIME =  90.0 // seconds

    private var _maxConnectionPoolCount: Int = DEFAULT_MAX_CONNECTION_POOL_COUNT
    public var maxConnectionPoolCount: Int {
        get {
            var result: Int!
            internalQueue.sync {
                result = self._maxConnectionPoolCount
            }
            return result
        }
    }
    private var _connectionPoolLifetime: TimeInterval = DEFAULT_CONNECTION_POOL_LIFETIME
    public var connectionPoolLifetime: TimeInterval {
        get {
            var result: TimeInterval!
            internalQueue.sync {
                result = self._connectionPoolLifetime
            }
            return result
        }
    }

    var connectionPoolTimer: DispatchSourceTimer
    var connectionPoolValues: [[String: NSValue]] = [[:]]
    var connectionPoolDates: [Date] = []

    public var db: OpaquePointer?

    public convenience init() {
        let file = "database3"
        self.init(file: file)
    }

    public convenience init(file: String) {
        let path = NSSearchPathForDirectoriesInDomains(.documentDirectory, .userDomainMask, true).first!.appending("/\(file).sqlite")
        self.init(path: path)
    }
    
    public init!(path: String,
                 serializer: @escaping DatabaseSerializer = defaultSerializer,
                 deserializer: @escaping DatabaseDeserializer = defaultDeserializer,
                 metadataSerializer: @escaping DatabaseSerializer = defaultSerializer,
                 metadataDeserializer: @escaping DatabaseDeserializer = defaultDeserializer) {
        self.databasePath = path
        self.objectSerializer = serializer
        self.objectDeserializer = deserializer
        self.metadataSerializer = metadataSerializer
        self.metadataDeserializer = metadataDeserializer

        // Initialize variables

        connectionPoolTimer = DispatchSource.makeTimerSource(queue: internalQueue)

        // Open database

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
            Daytabase.log.error("Error opening database")
            return nil
        }

        // Mark the queues so we can identify them.
        // There are several methods whose use is restricted to within a certain queue.

        snapshotQueue.setSpecific(key: isOnSnapshotQueueKey, value: false)
        writeQueue.setSpecific(key: isOnWriteQueueKey, value: false)

        // Complete database setup in the background

        snapshotQueue.async {
            autoreleasepool {
                self.upgradeTable()
                self.prepare()
            }
        }
    }


    // MARK: - Setup

    func openDatabase() -> Bool {
        let flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_NOMUTEX | SQLITE_OPEN_PRIVATECACHE
        let status = sqlite3_open_v2(databasePath, &db, flags, nil)

        if status == SQLITE_OK { return true }
    
        if let _ = db {
            Daytabase.log.error("Error opening database: \(status) \(daytabase_errmsg(self.db))")
        } else {
            Daytabase.log.error("Error opening database: \(status)")
        }
        return false
    }
    
    
    // TODO
    func configureDatabase(isNewDatabase: Bool) -> Bool {
        return true
    }
    
    func createTables() -> Bool {
        let createYapTableStatement =
            "CREATE TABLE IF NOT EXISTS \"\(extensionTableName)\"" +
            " (\"extension\" CHAR NOT NULL, " +
            "  \"key\" CHAR NOT NULL, " +
            "  \"data\" BLOB, " +
            "  PRIMARY KEY (\"extension\", \"key\")" +
            " );"
        if sqlite3_exec(db, createYapTableStatement, nil, nil, nil) != SQLITE_OK {
            Daytabase.log.error("Failed creating '\(extensionTableName)' table: \(daytabase_errmsg(self.db))")
            return false
        }
        
        let createDatabaseTableStatement =
            "CREATE TABLE IF NOT EXISTS \"\(defaultTableName)\"" +
            " (\"rowid\" INTEGER PRIMARY KEY," +
            "  \"collection\" CHAR NOT NULL," +
            "  \"key\" CHAR NOT NULL," +
            "  \"data\" BLOB," +
            "  \"metadata\" BLOB" +
            " );";
        if sqlite3_exec(db, createDatabaseTableStatement, nil, nil, nil) != SQLITE_OK {
            Daytabase.log.error("Failed creating '\(defaultTableName)' table: \(daytabase_errmsg(self.db))")
            return false
        }
      
        let createIndexStatement = "CREATE UNIQUE INDEX IF NOT EXISTS \"true_primary_key\" ON \"\(defaultTableName)\" ( \"collection\", \"key\" );"
        if sqlite3_exec(db, createIndexStatement, nil, nil, nil) != SQLITE_OK {
            Daytabase.log.error("Failed creating index on '\(defaultTableName)' table: \(daytabase_errmsg(self.db))")
            return false
        }
        return true
    }

    // MARK: - Utilities

    static func sqliteVersion(using db: OpaquePointer?) -> String {
        guard let statement = Database.getSqliteVersionStatement(with: db) else { return "Unknown" }
        defer { sqlite3_finalize(statement) }

        let status = sqlite3_step(statement)
        if status != SQLITE_ROW {
            Daytabase.log.error("Error executing 'sqliteVersion': \(status) \(daytabase_errmsg(db))")
        }

        guard let text = sqlite3_column_text(statement, SQLITE_COLUMN_START) else { return "Unknown" }
        return String(cString: text)
    }

    // MARK: - Upgrade

    func upgradeTable() {
    }

    // MARK: - Prepare

    func prepare() {
        beginTransaction()
        snapshot = readSnapshot()
        sqliteVersion = Database.sqliteVersion(using: db)
        Daytabase.log.verbose("sqlite version = \(self.sqliteVersion)")
        commitTransaction()
    }

    func beginTransaction() {
        guard let statement = beginTransactionStatement else { return }
        let status = sqlite3_step(statement)
        if status != SQLITE_DONE {
            Daytabase.log.error("Couldn't begin transaction: \(status) \(daytabase_errmsg(self.db))")
        }
        sqlite3_reset(statement)
    }

    func commitTransaction() {
        guard let statement = commitTransactionStatement else { return }
        let status = sqlite3_step(statement)
        if status != SQLITE_DONE {
            Daytabase.log.error("Couldn't commit transaction: \(status) \(daytabase_errmsg(self.db))")
        }
        sqlite3_reset(statement)
    }

    func readSnapshot() -> Int64 {
        guard let statement = readSnapshotInExtensionStatement else { return 0 }

        defer { sqlite3_finalize(statement) }

        let column_idx_data    = SQLITE_COLUMN_START
        let bind_idx_extension = SQLITE_BIND_START + 0
        let bind_idx_key       = SQLITE_BIND_START + 1

        let ext = ""
        let key = "snapshot"

        sqlite3_bind_text(statement, bind_idx_extension, ext, ext.length, SQLITE_STATIC)
        sqlite3_bind_text(statement, bind_idx_key, key, key.length, SQLITE_STATIC)

        let status = sqlite3_step(statement)
        if status != SQLITE_ROW {
            Daytabase.log.error("Error executing 'readSnapshot': \(status) \(daytabase_errmsg(self.db))")
            return 0
        }

        return sqlite3_column_int64(statement, column_idx_data)
    }

    func writeSnapshot() {
        guard let statement = writeSnapshotInExtensionStatement else { return }

        let bind_idx_extension = SQLITE_BIND_START + 0
        let bind_idx_key       = SQLITE_BIND_START + 1
        let bind_idx_data      = SQLITE_BIND_START + 2

        let ext = ""
        let key = "snapshot"
        sqlite3_bind_text(statement, bind_idx_extension, ext, ext.length, SQLITE_STATIC)
        sqlite3_bind_text(statement, bind_idx_key, key, key.length, SQLITE_STATIC)
        sqlite3_bind_int64(statement, bind_idx_data, snapshot);

        let status = sqlite3_step(statement)
        if status != SQLITE_DONE {
            Daytabase.log.error("Error in statement: \(status) \(daytabase_errmsg(self.db))")
        }
        
        sqlite3_finalize(statement)
    }
    
    // MARK: - Defaults
    
    public var defaultObjectCacheEnabled: Bool {
        get {
            var result: Bool = false
            internalQueue.sync {
                result = self._connectionDefaults.objectCacheEnabled
            }
            return result
        }
        set {
            internalQueue.sync {
                self._connectionDefaults.objectCacheEnabled = newValue
            }
        }
    }

    public var defaultObjectCacheLimit: Int {
        get {
            var result: Int = 0
            internalQueue.sync {
                result = self._connectionDefaults.objectCacheLimit
            }
            return result
        }
        set {
            internalQueue.sync {
                self._connectionDefaults.objectCacheLimit = newValue
            }
        }
    }
    
    public var defaultMetadataCacheEnabled: Bool {
        get {
            var result = false
            internalQueue.sync {
                result = self._connectionDefaults.metadataCacheEnabled
            }
            return result
        }
        set {
            internalQueue.sync {
                self._connectionDefaults.metadataCacheEnabled = newValue
            }
        }
    }
    
    public var defaultMetadataCacheLimit: Int {
        get {
            var result = 0
            internalQueue.sync {
                result = self._connectionDefaults.metadataCacheLimit
            }
            return result
        }
        set {
            internalQueue.sync {
                self._connectionDefaults.metadataCacheLimit = newValue
            }
        }
    }
    
    public var defaultObjectPolicy: DatabasePolicy {
        get {
            var result: DatabasePolicy = .share
            internalQueue.sync {
                result = self._connectionDefaults.objectPolicy
            }
            return result
        }
        set {
            internalQueue.sync {
                self._connectionDefaults.objectPolicy = newValue
            }
        }
    }
    
    public var defaultMetadataPolicy: DatabasePolicy {
        get {
            var result: DatabasePolicy = .share
            internalQueue.sync {
                result = self._connectionDefaults.metadataPolicy
            }
            return result
        }
        set {
            internalQueue.sync {
                self._connectionDefaults.metadataPolicy = newValue
            }
        }
    }
    
    // MARK: - Connections

    private func add(connection: Connection) {
        connection.connectionQueue.async {
            self.snapshotQueue.sync {
                let state = ConnectionState(connection: connection)
                self.connectionStates.append(state)
                Daytabase.log.verbose("Created new connection for <\(self): databaseName=\((self.databasePath as NSString).lastPathComponent), connectionCount=\(self.connectionStates.count)>")
                connection.prepare()
            }
        }
    }

    private func remove(connection: Connection) {
        let block = {
            var index = 0
            for state in self.connectionStates {
                if let stateConnection = state.connection,
                    stateConnection == connection {

                }
                index += 1
            }
            Daytabase.log.verbose("Removed connection from <\(self): databaseName=\((self.databasePath as NSString).lastPathComponent), connectionCount=\(self.connectionStates.count)>")
        }

        // We prefer to invoke this method synchronously.
        //
        // The connection may be the last object retaining the database.
        // It's easier to trace object deallocations when they happen in a predictable order.

        if snapshotQueue.getSpecific(key: isOnSnapshotQueueKey)! {
            block()
        } else {
            snapshotQueue.sync(execute: block)
        }
    }

    public func newConnection() -> Connection {
        let connection = Connection(database: self)
        add(connection: connection)
        return connection
    }

    // MARK: - Extensions

}
