#import "ImportSpeedTest.h"

@implementation ImportSpeedTest
{
@private
   NSString* _documentsDir;
   NSString* _dbPath;
   
   NSBundle* _mainBundle;
   NSBundle* _testBundle;
   
   NSDictionary* _schema;
}

-(NSString*)pathForDatasetNamed:(NSString*)csvFileName
{
   NSString* result = [ self->_testBundle pathForResource: csvFileName
                                                   ofType: @"csv" ];
   return result;
}

-(void)cleanupFS
{
   NSArray *paths = NSSearchPathForDirectoriesInDomains(NSDocumentDirectory, NSUserDomainMask, YES);

   self->_documentsDir = paths[0];
   self->_dbPath = [ self->_documentsDir stringByAppendingPathComponent: @"ImportSpeedTest.sqlite"];
   
   NSFileManager* fm = [ NSFileManager defaultManager ];
   [ fm removeItemAtPath: self->_dbPath
                   error: NULL ];
}

-(void)setUp
{
   [ super setUp ];
   [ self cleanupFS ];
   
   self->_mainBundle = [ NSBundle bundleForClass: [ self class ] ];
   self->_testBundle = self->_mainBundle;
   
   self->_schema = @{
                       @"Date"     : @"DATETIME"
                     , @"Visits"   : @"INTEGER"
                     , @"Value"    : @"INTEGER"
                     , @"FacetName": @"VARCHAR"
                     , @"FacetId"  : @"VARCHAR"
                     };
}

-(void)tearDown
{
   [ self cleanupFS ];
   [ super tearDown ];   
}

-(void)testImportData
{
   GHAssertNotNil( self->_dbPath, @"_dbPath mismatch" );
   GHAssertNotNil( self->_documentsDir, @"_documentsDir mismatch" );
   
   NSError*  error_    = nil;
   
   NSString* csvPath = [ self pathForDatasetNamed: @"1-RS-TT-All" ];
   
   CsvToSqlite* converter_ = [ [ CsvToSqlite alloc ] initWithDatabaseName: self->_dbPath
                                                             dataFileName: csvPath
                                                           databaseSchema: self->_schema
                                                               primaryKey: nil ];
   converter_.csvDateFormat = @"yyyyMMdd";
   GHAssertNotNil( converter_.dbWrapper, @"DB initialization error ");
   
   
   [ converter_  storeDataInTable: @"TrafficReferringSites"
                            error: &error_ ];
   GHAssertNil( error_, @"Unexpected error" );
   
   
   id<ESReadOnlyDbWrapper> dbWrapper_ = (id<ESReadOnlyDbWrapper>)converter_.dbWrapper;
   [ dbWrapper_ open ];
   NSInteger itemsCount_ = [ dbWrapper_ selectIntScalar: @"SELECT COUNT(*) FROM TrafficReferringSites" ];
   [ dbWrapper_ close ];
   
   GHAssertTrue( 89841 == itemsCount_, @"database mismatch" );
}

-(void)testImportData_AnsiDates
{
   GHAssertNotNil( self->_dbPath, @"_dbPath mismatch" );
   GHAssertNotNil( self->_documentsDir, @"_documentsDir mismatch" );
   
   NSError*  error_    = nil;
   
   NSString* csvPath = [ self pathForDatasetNamed: @"1-RS-TT-All-SqliteDate" ];
   
   CsvToSqlite* converter_ = [ [ CsvToSqlite alloc ] initWithDatabaseName: self->_dbPath
                                                             dataFileName: csvPath
                                                           databaseSchema: self->_schema
                                                               primaryKey: nil ];
   converter_.csvDateFormat = @"yyyy-MM-dd";
   GHAssertNotNil( converter_.dbWrapper, @"DB initialization error ");
   
   
   [ converter_  storeDataInTable: @"TrafficReferringSites"
                            error: &error_ ];
   GHAssertNil( error_, @"Unexpected error" );
   
   
   id<ESReadOnlyDbWrapper> dbWrapper_ = (id<ESReadOnlyDbWrapper>)converter_.dbWrapper;
   [ dbWrapper_ open ];
   NSInteger itemsCount_ = [ dbWrapper_ selectIntScalar: @"SELECT COUNT(*) FROM TrafficReferringSites" ];
   [ dbWrapper_ close ];
   
   GHAssertTrue( 89841 == itemsCount_, @"database mismatch" );
}


@end
