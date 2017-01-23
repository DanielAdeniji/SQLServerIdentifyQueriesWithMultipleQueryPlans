use [master]
go

if object_id('[dbo].[sp_IdentifyQueriesWithMultipleQueryPlans]') is null
begin

	exec('create procedure [dbo].[sp_IdentifyQueriesWithMultipleQueryPlans] as ');

end
go

alter procedure [dbo].[sp_IdentifyQueriesWithMultipleQueryPlans] 
(
	  @database sysname = null
	, @includeSummary bit = 1
)
as

begin

	/*

		1) 2017-01-21 dadeniji

		   Ran Stored Procedure ( SP ) and noticed the error below:

				Msg 8152, Level 16, State 10, Procedure sp_IdentifyQueriesWithMultipleQueryPlans
				String or binary data would be truncated.
				The statement has been terminated.

		   Change [queryText] definition from nvarchar(4000) to nvarchar(max)

	*/
	set nocount on;
	set transaction isolation level read uncommitted;

	declare @tblPlanCache TABLE
	(
		  [plan_handle] varbinary(64) not null
		, [objtype]		sysname not null
		, [size_in_bytes] int not null

		, primary key
			(
				[plan_handle]
			)
	)

	declare @tblCacheDistinct TABLE
	(
		  [queryHash]		Binary(8)
		, [objtype]			sysname
		, [planHandle]		varbinary(64)
		, [sqlHandle]  		varbinary(64) null
		, [planCount]		int

		, [sizeInBytesTotal]	bigint
		, [sizeInBytesAverage]	bigint

		, primary key
			(
				[queryHash]
			)

	)


	declare @cteDetail TABLE
	(
		  [queryHash]		Binary(8)
		, [planHandle]		varbinary(64)
		, [sqlHandle]		varbinary(64) null
		, [dbid]			int
		, [database]		sysname
		, [objtype]			sysname
		, [object]			sysname null
		, [queryText]		nvarchar(max)
		, [queryPlan]		xml null
		, [planCount]		int

		, [sizeInBytesTotal]	bigint
		, [sizeInBytesTotalMB]
			as [sizeInBytesTotal] / ( 1024 * 1024)
		
		, [sizeInBytesAverage]	bigint
		, [sizeInBytesAverageKB]
			as ( [sizeInBytesAverage] / 1024)

		, primary key
			(
				[queryHash]
			)

		, UNIQUE
			(
				 [dbid]
			   , [database]
			   , [queryHash]

			)
			
	)

	declare @cteSummary TABLE
	(
		  [dbid]			int
		, [database]		sysname
		, [planCount]		int

		, [sizeInBytesTotal]	bigint
		, [sizeInBytesTotalMB]
			as [sizeInBytesTotal] / ( 1024 * 1024)
		
		, [sizeInBytesAverage]	bigint
		, [sizeInBytesAverageKB]
			as ( [sizeInBytesAverage] / 1024)
	
	)

	declare @SQLTextTruncatedLength tinyint

	set @SQLTextTruncatedLength = 255

	insert into @tblPlanCache
	(
		  [plan_handle]
		, [size_in_bytes] 
		, [objtype]
	)
	select 
			    tblCP.plan_handle
			  , tblCP.[size_in_bytes] 
			  , tblCP.objtype
	from   sys.dm_exec_cached_plans tblCP
	

	insert into @tblCacheDistinct 
	(
		  [queryHash]		
		, [objtype]		
		, [planHandle]
		, [sqlHandle]		
		, [planCount]
		, [sizeInBytesTotal]
	  	, [sizeInBytesAverage]

	)
	select
		    [query_hash]
				= tblQS.[query_hash]

		  , [objtype]
				= min
				(
					tblCP.[objtype]
				)

		  , [plan_handle]
				= min(tblQS.[plan_handle])
			
		  ,	[sql_handle]
				= min
					(
						tblQS.[sql_handle]
					)		
		  		
		  , [count]
				= count(tblQS.plan_handle)	 

		  , [sizeInBytesTotal]
			= sum
				(
				cast (tblCP.[size_in_bytes] as bigint)
				)

	  	 , [sizeInBytesAverage]
			= avg
				(
				cast(tblCP.[size_in_bytes] as bigint )
				)

	FROM sys.dm_exec_query_stats tblQS

	INNER JOIN  @tblPlanCache tblCP 
			on tblCP.plan_handle = tblQS.[plan_handle]

	GROUP BY 
			    tblQS.[query_hash]
			  
	HAVING
			COUNT
			(
				tblQS.query_hash
			) > 1

	OPTION ( MAXDOP 1, RECOMPILE) 


	; with cte
	(
		  [queryHash]
		, [planHandle]
		, [sqlHandle]
		, [dbid]
		, [database]
		, [objtype]
		, [object]	
		, [queryText]
		, [queryPlan]
		, [planCount]
		, [sizeInBytesTotal]
		, [sizeInBytesAverage]

	)
	as
	(

		SELECT

				  tblCP.queryHash

				, [plan_handle]
					= tblCP.planHandle

				, [sql_handle]
					= (tblCP.[sqlHandle])
				
				, [dbid]
				   = cast(tblDEPA_DB.[value] as int)

				, [database]
					= case cast(tblDEPA_DB.[value] as int)
						when 32767 then 'Resource DB'
						else db_name(cast(tblDEPA_DB.[value] as int))
					  end


				, [objtype]
					= tblCP.objtype

				, [object]
					= 
						object_schema_name
						(
							  [st].[objectID]
							, [st].[dbID]
						)
						+ '.'
						+ object_name
						(
							 [st].[objectID]
						   , [st].[dbid]
						)

				, [queryText]
					= st.[text]

				, [query_plan]

				, PlanCount
					= tblCP.planCount

				, tblCP.[sizeInBytesTotal]

	  			, tblCP.[sizeInBytesAverage]

		
		FROM @tblCacheDistinct tblCP

		CROSS APPLY sys.dm_exec_query_plan(tblCP.planHandle) AS qp

		cross APPLY sys.dm_exec_plan_attributes(tblCP.planHandle) as tblDEPA_DB

		CROSS APPLY sys.dm_exec_sql_text(tblCP.[sqlHandle]) AS st
	
		WHERE tblDEPA_DB.[attribute] = 'dbid'

		AND   st.[text] not like '%sys.%'
	
		AND   (
					   (@database is null )
					or (db_id(@database) is null )
					or cast(tblDEPA_DB.[value] as int) = db_id(@database)
					
			  )
	)
	insert into @cteDetail
	select *
	from   cte

	OPTION ( MAXDOP 1, RECOMPILE) 


	insert into @cteSummary
	(

		  [dbid]
		, [database]
		, planCount
		, [sizeInBytesTotal]
	)
	select

			  cte.[dbid]
			, cte.[database]
			, plancount
				= sum(cte.planCount)
			, [sizeInBytesTotal]
				=sum([sizeInBytesTotal])

	from   @cteDetail cte

	group by
			   [dbid]
			 , [database]

	OPTION ( MAXDOP 1, RECOMPILE) 

	select
		  [queryHash]	
		, [planHandle]	
		, [sqlHandle]		
		--, [dbid]			
		, [database]
		, [objtype]	
		, [object]	
		, [planCount]	

		--, [sizeInBytesTotal]
		, [sizeInBytesTotalMB]
			
		--, [sizeInBytesAverage]	
		, [sizeInBytesAverageKB]

		, [queryTextTruncated]	
			= left(
					  [queryText]
					, @SQLTextTruncatedLength
				  )

		, [queryText]	

		, [queryPlan]	
				

	from   @cteDetail cte 

	order by
				cte.[planCount] desc

	if (@includeSummary = 1)
	begin

		select
				  cte.[database]

				, cte.[sizeInBytesTotalMB]

				, cte.[planCount]

				
		from   @cteSummary cte

		order by
	
					  cte.[sizeInBytesTotalMB] desc

					, cte.[planCount] desc

	end

end
go
