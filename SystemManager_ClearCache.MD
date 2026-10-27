#Parameters
[SMParameters]
SMTPSERVER=smtp.servername.int
SMTPPORT=
BASEDIR=C:\MicroStrategy\SystemManager\MarketIntelligence_ClearCache\
DBLOGIN=dbuser
DBPWD=password
DropCache=
MIDWHLoadHistoryIds=
MSTRLogin=administrator
MSTRProjectSource=Market Intelligence Stage
MSTRPwd=
NotificationEmail=AltusAdministrators@email.com
SRCDSN=MIDWH


#Run SQL CheckIfMIDWHRefreshHappened.sql

SELECT TOP 1
      case when MIDWHLoadHistoryId is not null then 1 else 0 end
  FROM [BPIPlus].[MicroStrategy].[MIDWHLoadHistory]
  WHERE DropMSCacheDataTime is null
  GROUP BY [MIDWHLoadHistoryId], DropMSCacheDataTime
  ORDER BY [MIDWHLoadHistoryId] DESC
  ;
  
##Failure: Run SQL CheckIfMIDWHRefreshHappened.sql 
If there is a failure to retrieve anything send an email to the Administrator
High Importance
Subject: MarketIntelligence ClearCache - SQL Failed
Message Body: The Stored Procedure CheckIfMIDWHRefreshHappened has failed.
Attach log file
  
#Success: Decision
If result = 1 then continue else exit

#Run CheckIfMIDWHRefreshHappened_RetrieveIDs.sql
  select substring(
  (SELECT ',' + convert(varchar(10),MIDWHLoadHistoryId)  AS 'data()'
                                             FROM BPIPlus.MicroStrategy.MIDWHLoadHistory 
                                             WHERE DropMSCacheDataTime IS  NULL
                                             FOR XML PATH('')) ,2,9999) MILoadIDs;
Outputs: ${MIDWHLOADHISTORYIDS}

CREATE PROCEDURE MicroStrategy.IndicateCacheDropHappened
              @MIDWHLoadHistoryIds    varchar(max)
AS
BEGIN
    SET NOCOUNT ON
    DECLARE @SchemaName         sysname 
           ,@DatabaseName       sysname = DB_NAME()
           ,@ServerName         sysname = @@ServerName
           ,@Trancount          integer = @@TRANCOUNT
		         ,@ProcName           sysname = Object_Name(@@ProcId)
            --
           ,@rowcount           integer

    DECLARE @Ids TABLE
        (IdValue      integer)

    SELECT @SchemaName = ss.name
    FROM   sys.objects so
        INNER JOIN sys.schemas ss ON ss.schema_id = so.schema_id
    WHERE  so.object_id = @@PROCID

    BEGIN TRY

      IF LEN(ISNULL(@MIDWHLoadHistoryIds,'')) > 0
      BEGIN

      				INSERT INTO @Ids
	    	  		     (IdValue)
	    			  SELECT Split.Value.value('.', 'varchar(20)') AS String
      				FROM  (SELECT CAST('<M>' + REPLACE(@MIDWHLoadHistoryIds,',','</M><M>') + '</M>' AS XML) AS String) AS Value
	    	  		    CROSS APPLY String.nodes('/M') AS Split(Value)
     
            IF @Trancount = 0
                BEGIN TRANSACTION
     
            UPDATE mlh
                SET mlh.DropMSCacheDataTime = sysdatetime()
            FROM MicroStrategy.MIDWHLoadHistory mlh
                INNER JOIN @Ids i ON i.IdValue = mlh.MIDWHLoadHistoryId
            WHERE mlh.DropMSCacheDataTime IS NULL
     
     
            IF @Trancount < @@TRANCOUNT
                COMMIT TRANSACTION
        END

    END TRY
    BEGIN CATCH
        

       EXECUTE dbo.RaiseErrorInformation
                  @SchemaName      = @SchemaName
                 ,@DatabaseName    = @DatabaseName
                 ,@ServerName      = @ServerName
                 ,@ProcName        = @ProcName

       IF @Trancount < @@TRANCOUNT
           ROLLBACK TRANSACTION          
    
    END CATCH
END
go
											 
#Intelligence Server - ClearCache
INVALIDATE ALL REPORT CACHES IN PROJECT "Crop Canada";

#Run IndicateCacheDropHappened
EXEC BPIPlus.MicroStrategy.IndicateCacheDropHappened '${MIDWHLOADHISTORYIDS}';

#Success Intelligence Server - Executes Cache refresh subscription and notifies users on refresh.
TRIGGER EVENT "Refresh-Crop-Canada-Cubes Trigger"; --Refresh Cubes
On Success
TRIGGER EVENT "NotifyClearCache"; --Refresh reports, notifies users.


#Failure Send Email - Admin Fail
Subject: MarketIntelligence ClearCache - SQL Failed
Body: The Stored Prodedure IndicateCacheDropHappened has failed.