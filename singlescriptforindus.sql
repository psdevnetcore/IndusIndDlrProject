USE [indus_2]
GO
SET NOCOUNT ON

	DECLARE @bat   INT
	DECLARE @path     varchar(255)
DEClARE @log_file_name varchar(50)
	
-- set batch variable as per number of records to be updated on uat or prod
  SET @bat   = 100000
SET @log_file_name = 'índus_2_log'
SET @path = 'C:\Users\netcore\Desktop\IndusInd\stats\'

dbcc SHRINKFILE (@log_file_name, 1) WITH NO_INFOMSGS;

IF (SELECT COUNT(*) FROM sys.objects WHERE object_id = object_id('dlr_tmp')) = 1
BEGIN
	DROP TABLE dlr_tmp;
END

	--- Start Create DLR TEMP
	CREATE TABLE [dbo].[dlr_tmp](
		[Message_Id] [varchar](50) NULL,
		[DELIVERY_STATUS] [varchar](50) NULL,
		[DELIVERY_TIME] [varchar](50) NULL
	) ON [PRIMARY]
	--- End Create DLR TEMP

IF (SELECT COUNT(*) FROM sys.objects WHERE name = 'dlr_update_tmp_log') = 0
BEGIN
	--- Start Create DLR TEMP LOG
	CREATE TABLE [dbo].[dlr_update_tmp_log](
		[from_record_limit] [varchar](50) NULL,
		[to_record_limit] [varchar](50) NULL,
		[Status] [varchar](50) NULL,
		[log_time] [varchar](50) NULL,
		[tablename] [varchar](50) NULL,
		[rowcount] [varchar](50) NULL,
		[pk] [int] IDENTITY(1,1) NOT NULL
	) ON [PRIMARY] 
END

	

IF (SELECT COUNT(*) FROM sys.objects WHERE name = 'ALLFILENAMES') = 1
BEGIN
	DROP TABLE ALLFILENAMES;
END

	CREATE TABLE ALLFILENAMES(pk int identity (1,1), WHICHPATH VARCHAR(255),WHICHFILE varchar(255))

-- Start BULK INSERT
 --a table to loop thru filenames drop table ALLFILENAMES
    
 
    --some variables
    declare @filename varchar(255),
            
            @sql      varchar(8000),
            @cmd      varchar(1000)
DECLARE @sqlCommand nvarchar(1000)
	DECLARE @ctr_1 INT
	DECLARE @ctr_2 INT

	DECLARE @maxpk INT
 
 
    --get the list of files to process:
    
    SET @cmd = 'dir "' + @path + '*.csv" /b'
--    print @cmd
    INSERT INTO  ALLFILENAMES(WHICHFILE)
    EXEC Master..xp_cmdShell @cmd
    
    
    
    UPDATE ALLFILENAMES SET WHICHPATH = @path where WHICHPATH is null
    
    DELETE ALLFILENAMES WHERE WHICHFILE IS NULL
--    select * from ALLFILENAMES

--cursor loop
    declare c1 cursor for SELECT WHICHPATH,WHICHFILE FROM ALLFILENAMES where WHICHFILE like '%.csv%' order by pk
    open c1
    fetch next from c1 into @path,@filename
    While @@fetch_status <> -1
      begin
      --bulk insert won't take a variable name, so make a sql and execute it instead:

           set @sql = ' BEGIN
					truncate table dlr_tmp;
					if (select count(*) from sys.columns where name = ''pk'' and object_id = object_id(''dlr_tmp'')) = 1
						BEGIN
							 alter table dlr_tmp drop column pk ;
							 
						END
					END
					'
		exec (@sql)
       
       set @sql = 'begin tran bi 
				BULK INSERT dbo.dlr_tmp  FROM ''' + @path + @filename + ''' '
           + '     WITH ( 
                   FIELDTERMINATOR = ''|'', 
                   ROWTERMINATOR = ''0x0a'' 
                ); 
				commit tran bi'
--    print @sql

    exec (@sql)
    
    dbcc SHRINKFILE (@log_file_name, 1) WITH NO_INFOMSGS;
    ---- dbcc SHRINKFILE('@log_file_name', 0, TRUNCATEONLY) WITH NO_INFOMSGS
    
           set @sql = ' BEGIN
					if (select count(*) from sys.columns where name = ''pk'' and object_id = object_id(''dlr_tmp'')) = 0
						BEGIN
							 alter table dlr_tmp add pk int identity(1,1);
						END
					END
					'
		
		exec (@sql)
  
	set @sqlCommand = 'SELECT @maxpkey = MAX(pk) FROM dbo.dlr_tmp;'

EXECUTE sp_executesql @sqlCommand, N'@maxpkey int OUTPUT', @maxpkey = @maxpk OUTPUT

  insert into dlr_update_tmp_log values(-1, -1, 'File upload' ,GETDATE(),@filename ,@maxpk);
    
  
SET @ctr_1 = 1
SET @ctr_2 = @bat

WHILE @maxpk >= @ctr_1

    BEGIN 
            insert into dlr_update_tmp_log values(@ctr_1, @ctr_2, 'Update Start ' ,GETDATE(), 'GupShup_Promo_Outgoing_SMS',0);
      
            BEGIN TRAN t1;
          
            UPDATE    mst
            SET        mst.[DELIVERY_STATUS]    =    tmp.[DELIVERY_STATUS],
                    mst.[DELIVERY_TIME]        =    tmp.[DELIVERY_TIME]
            FROM    [GupShup_Promo_Outgoing_SMS] mst, dlr_tmp tmp
            WHERE    mst.[Message_Id]        =    tmp.[Message_Id]
            AND        tmp.pk    BETWEEN    @ctr_1 AND @ctr_2
       
            insert into dlr_update_tmp_log values(@ctr_1, @ctr_2, 'Update Finish ' ,GETDATE(), 'GupShup_Promo_Outgoing_SMS',@@rowcount);
      
            COMMIT TRAN t1;
            
            dbcc SHRINKFILE (@log_file_name, 1) WITH NO_INFOMSGS;
			---- dbcc SHRINKFILE('@log_file_name', 0, TRUNCATEONLY) WITH NO_INFOMSGS
            

     /*
       --------------Update table2 
            insert into dlr_update_tmp_log values(@ctr_1, @ctr_2, 'Update Start ' ,GETDATE(), 'Netcore_Promo_Outgoing_SMS', 0);
      
            BEGIN TRAN t2;
          
            UPDATE    mst
            SET        mst.[DELIVERY_STATUS]    =    tmp.[DELIVERY_STATUS],
                    mst.[DELIVERY_TIME]        =    tmp.[DELIVERY_TIME]
            FROM    [Netcore_Promo_Outgoing_SMS] mst, dlr_tmp tmp
            WHERE    mst.[Message_Id]        =    tmp.[Message_Id]
            AND        tmp.pk    BETWEEN    @ctr_1 AND @ctr_2
      
           insert into dlr_update_tmp_log values(@ctr_1, @ctr_2, 'Update Finish ' ,GETDATE(), 'Netcore_Promo_Outgoing_SMS',@@rowcount);
            COMMIT TRAN t2; 
			  dbcc SHRINKFILE (@log_file_name, 1) WITH NO_INFOMSGS;
--			  -- dbcc SHRINKFILE('@log_file_name', 0, TRUNCATEONLY) WITH NO_INFOMSGS
            
           
             
         --------------Update table3 
            insert into dlr_update_tmp_log values(@ctr_1, @ctr_2, 'Update Start ' ,GETDATE(), 'Netcore_Promo_Outgoing_SMS_NOV_2016', 0);
      
            BEGIN TRAN t3;
          
            UPDATE    mst
            SET        mst.[DELIVERY_STATUS]    =    tmp.[DELIVERY_STATUS],
                    mst.[DELIVERY_TIME]        =    tmp.[DELIVERY_TIME]
            FROM    [Netcore_Promo_Outgoing_SMS_NOV_2016] mst, dlr_tmp tmp
            WHERE    mst.[Message_Id]        =    tmp.[Message_Id]
            AND        tmp.pk    BETWEEN    @ctr_1 AND @ctr_2
      
		insert into dlr_update_tmp_log values(@ctr_1, @ctr_2, 'Update Finish ' ,GETDATE(), 'Netcore_Promo_Outgoing_SMS_NOV_2016',@@rowcount);
      
            COMMIT TRAN t3;
            
            
			dbcc SHRINKFILE (@log_file_name, 1) WITH NO_INFOMSGS;
			-- -- dbcc SHRINKFILE('@log_file_name', 0, TRUNCATEONLY) WITH NO_INFOMSGS
           */
             
             
            SET @ctr_1 = @ctr_1 + @bat
            SET @ctr_2 = @ctr_2 + @bat
      
      
      
      
      
    END
  
      fetch next from c1 into @path,@filename
      end
    close c1
    deallocate c1
 
 
  
