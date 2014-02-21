
##### Ini-Suquence: sifteR ################################################

library(RODBC) 

# load("C:\\Users\\Owner\\Desktop\\HPN R Directory\\Gemini_Model.RData")

# 'c:\Program Files\Microsoft SQL Server\
	# MSSQL10_50.SQLEXPRESS\MSSQL\DATA\sifter2.mdf

#Source Functions
  px <- function(...) { paste(..., sep = "") }
  ps <- function(...) { paste(..., sep = " ") }
  py <- function(...) { paste(..., sep = ";") }


## Make Database-Connection. Note - DSN string needs all spaces removed ------

  db <- 'sifter2'
  conn.str <- py(    "driver=SQL Server", px("database=", db) 
          	        ,"server=Owner-PC\\SQLEXPRESS;" )
  conn <- odbcDriverConnect(conn.str)

  db.2 <- 'hpn2'
  conn.str <- py(    "driver=SQL Server" ,px("database=", db.2) 
          	        ,"server=Owner-PC\\SQLEXPRESS;" )
  conn.2 <- odbcDriverConnect(conn.str)


## DB Management and global environ -----------------------------------------
  
check.drop <- function(x) {
	  db.tbls <- sqlQuery(conn,
		        "SELECT * FROM sysobjects WHERE xtype = 'U' ")[,1]
	  if ( x %in% db.tbls ) { print('dropped');
		 sqlQuery(conn, paste('DROP TABLE ', x)) 		 } }

getSqlTypeInfo(odbcGetInfo(conn)[1])
sqlTypeInfo(conn.2)
sqlColumns(conn.2, "exampR1")
qqq <- "ALTER TABLE exampR1 ALTER COLUMN Zmod int"
sqlQuery(conn.2,qqq)
xx<-sqlTables(conn)




## Copy ClaimsT1 and four QF Mapping tbls into new database ------------------

  db.dest <- 'sifter2.dbo.' 
  db.orig <- 'hpn2.dbo.'
  tbl.vec <- c('ClaimsT1', 'PlaceSvcTbl', 'ProcGTbl', 
				   'SpecialtyTbl' , 'PCGTbl'  )

  for (i in 1:5) { 

      sqlQuery( conn.2, ps( 'SELECT * INTO' , c( db.dest, tbl.vec[i] ),
			   		 'FROM', c(db.orig, tbl.vec[i] )          ) )}


## Refine & Import qf.tbl's -------------------------------------------------

  qf.tbl <- list()	
  tbl.vec <- c('PlaceSvcTbl', 'ProcGTbl' , 'SpecialtyTbl' , 'PCGTbl' )

  for (i in 1:4) { 	

	sqlQuery(conn, ps('ALTER TABLE', tbl.vec[i], 
				'ALTER COLUMN RankA tinyint')  ) 

	qf.tbl[[i]] <- sqlQuery( conn, ps('SELECT * FROM',tbl.vec[i]) ) 	}



## Create Mapping Tables for Date Logic -----------------------------------

q.str <- 'SELECT MapMY,Dmax=Max(dsfsT) INTO DmaxTbl FROM claimst1 GROUP BY MapMy'
check.drop('DmaxTbl')
sqlQuery(conn, q.str)



## Create TT: var selection and qf mapping ---------------------------------

system.time({

check.drop('TTx')

q.str <- {'CREATE TABLE TTx (  mapmy int, pst tinyint, spect tinyint,
                              pcgt tinyint, proct tinyint, lost tinyint,
					date1 tinyint, id1 smallint, id2 smallint)'}
sqlQuery(conn, q.str)

q.str <- {  'INSERT INTO TTx 
		 SELECT mapmy = t.mapmy, 
		 pst = qf1.RankA, spect = qf2.RankA, 
		 pcgt = qf3.RankA, proct = qf4.RankA, 
		 lost = case when t.LOST1 > 0 then 2 else 1 end,
		 date1 = case 
			   when (d.Dmax - t.dsfsT)=0 THEN 1 
			   when (d.Dmax - t.dsfsT)>0 AND (d.Dmax - t.dsfsT)<=3 THEN 2				 
			   else 3
			   end,
		 id1 = t.VendMap, id2 = t.ProvMap		 

		FROM claimst1 t
		LEFT JOIN placesvctbl qf1 on t.PlaceSvc = qf1.PlaceSvc
		LEFT JOIN specialtytbl qf2 on t.Specialty = qf2.Specialty
		LEFT JOIN pcgtbl qf3 on t.PrimaryConditionGroup
							 = qf3.PrimaryConditionGroup
		LEFT JOIN procgtbl qf4 on t.ProcedureGroup
							 = qf4.ProcG
		LEFT JOIN DmaxTbl d on t.MapMY = d.MapMY

		WHERE t.YearT <> 4
		'}

sqlQuery(conn, q.str)

})


### Instance: sifteR ##########################################################

library(RODBC)

#Source Functions
  px <- function(...) { paste(..., sep = "") }
  ps <- function(...) { paste(..., sep = " ") }
  py <- function(...) { paste(..., sep = ";") }

#Establish connection to DB
  db <- 'sifter2'
  conn.str <- py(    "driver=SQL Server", px("database=", db) 
          	        ,"server=Owner-PC\\SQLEXPRESS;" )
  conn <- odbcDriverConnect(conn.str)

  check.drop <- function(x) {
	  db.tbls <- sqlQuery(conn,
		        "SELECT * FROM sysobjects WHERE xtype = 'U' ")[,1]
	  if ( x %in% db.tbls ) { print('dropped');
		 sqlQuery(conn, paste('DROP TABLE ', x)) 		 } }


#####Loop over sift.mains index, sift.ind -------------------------------------



  #Define Looping Parameters ##

   sift.ind <- 8
   y.ind   <-  4		 # (aka t.1) col index for target.main 
   n.ind   <-  3		 # (aka d.1) member index for train set
   var.min  <- 2		 # min number of permN in PT name
   z.power  <- 0.5  	 # p in power(sum(nhat),p)*(yhat-ypop)
   top.x    <- 20    	 # import top x rows
   min.n    <- 5		 # min count PT group for import
   topsift.x <- 200	 # variables to import
   agg.func <- 'SUM'	 # for sql statement
   ptagg.distinct <- 2   # factor out duplicates of MapMY x GroupBy 
   var.prefix <- 'preg'	 # for  iterative generation of sifting
   i.1      <- 5		 # for fitrows indx

##Update DB with table of MapMY's and Y-values -------------------------------

  # union( KN-target.base + KP-target.main)

  samp.domain <- which( (target.base[,n.ind] | target.main[,y.ind ]) == T)
  samp.doamin <- intersect(fit.ind[[i.1]], samp.domain )
  temp.vec <- rep(0,nrow(data.model))
  temp.vec[samp.domain] <- 1


  #Build mat to export
  sift.input <- data.frame( cbind( 
			mapmy = data.model$MapMY, 
			y     = target.main[ , y.ind], 
			n     = temp.vec			)     )

#Import to score
system.time( {
  check.drop('MBYscoreTbl')
  sqlSave(   conn
		,dat = sift.input
		,tablename = 'MBYscoreTbl'
		,append = FALSE
		#,typeInfo = 'integer', 'integer', 'integer'
		,varTypes = c( mapmy = 'int', y = 'float', n = 'float' )
		,colnames = FALSE
		,rownames = FALSE
		,fast = TRUE						)
  })

# Map y-values onto TT
  system.time( {
    check.drop('TT')
    q.str <- {'CREATE TABLE TT (  mapmy int, pst tinyint, spect tinyint,
                              pcgt tinyint, proct tinyint, lost tinyint,
					date1 tinyint, id1 smallint, id2 smallint,
					y float, n float )'}
    sqlQuery(conn, q.str)

    q.str <-{'INSERT INTO TT 
		  SELECT TTx.*, score.y, score.n 
		  FROM TTx
		  LEFT JOIN MBYscoreTbl score 
		  ON TTx.mapmy = score.mapmy
		  WHERE score.n = 1'}

    sqlQuery( conn, q.str )
  })


## Generate Permutations for PT tables -------------------------------------

library(gregmisc)

  qf.names <-sqlColumns(conn,'TT')['COLUMN_NAME']
  qf.names <-data.frame(cbind(ind = (1:(dim(qf.names)[1])),
				fields = qf.names) )

  clinical.ind <- c(2:5) #6)
  date.ind <-     c(7) #,7)
  id.ind  <-      c(8)
  perm.ind <- c( clinical.ind ) # ,id.ind )  #, date.ind, id.ind)
  var.max <- min( 6, length(perm.ind) )

  PT.map <- matrix(cbind(map=1:200,n=0,i=0), nrow = 200, ncol =3)
  PT.ind <- list()
  i <- 1

  for (r.loop in var.min:var.max) {  
	PT.ind[[r.loop]] <- combinations( n = length(perm.ind), r = r.loop,
						    v = perm.ind )  		
	rows.add <- dim(PT.ind[[r.loop]])[1]
      PT.map[i:(i+rows.add-1),2] <- matrix(rep(r.loop,rows.add))
      PT.map[i:(i+rows.add-1),3] <- matrix((1:rows.add))
	i <- i + rows.add; 
  }	

## Create & Execute PT Select Statement --------------------------------------------------

y.pop <- sqlQuery(conn, 'SELECT y= SUM(y), n = SUM(n) FROM TT WHERE n = 1')
y.pop <- y.pop[1]/y.pop[2]

sd.pop <-  sqrt(y.pop*(1-y.pop))
#sd.pop <- as.numeric(sqlQuery(conn, 'SELECT y= Stdev(y) FROM TT'))

system.time({
 for (r.iter in var.min:var.max) { 

    PT.tbl <- PT.ind[[r.iter]]
    PT.n <- dim(PT.tbl)[1]
    #n.iter<-2;r.iter<-3
    
    for (n.iter in (1:PT.n)) {

      PT.name <- paste( PT.tbl[n.iter,], collapse = '')
      PT.tblname <- paste('PT', PT.name, sep = '')
      GroupBy.fields <- paste(qf.names[PT.tbl[n.iter,],2],collapse = ', ')

      q.str <- paste(' SELECT ', GroupBy.fields, 
			   ', y= ( ( 1.0 * SUM(tt.y) ) / ( .0001+SUM(tt.n) ) ), ',
			   ' n=SUM(tt.n), ',
			   ' z = (( (1.0 * sum(tt.y )) / (.0001+sum(tt.n)) ) - ',
 			        y.pop, ' )*(power(sum(tt.n),', z.power, ')/', 
			        sd.pop, ') ', 
			   ' INTO ', PT.tblname) 
	
	if (ptagg.distinct == 1) { q.str <- paste( q.str,	
	   		   ' FROM TT GROUP BY ', GroupBy.fields ) }

      if (ptagg.distinct == 2) { q.str <- paste( q.str,
			         ' FROM (SELECT DISTINCT MapMY, ', GroupBy.fields, 
				   '       , y = MAX(y), n = MAX(n) ', 
				   '       FROM TT GROUP BY MapMY,', GroupBy.fields,
				   '        ) AS tt ' ,
				   ' GROUP BY ', GroupBy.fields ) }

    check.drop(PT.tblname)
    print(paste(r.iter, n.iter, system.time(sqlQuery(conn, q.str))[3] ) ) 
     }
  }
})

## Import TopX ------------------------------------------------------------

Top.Out <- list()
i <- 0

system.time({
for (r.iter in var.min:var.max) {

  PT.tbl <- PT.ind[[r.iter]]
  PT.n <- dim(PT.tbl)[1]

  for (n.iter in 1:PT.n) {

    i <- i + 1
    PT.name <- paste( PT.tbl[n.iter,], collapse = '')
    PT.tblname <- paste('PT', PT.name, sep = '')

    q.str <- paste(' SELECT Top ', top.x, ' * ', 
			 ' FROM ' , PT.tblname, 
			 ' WHERE n > ' , min.n,  
			 ' ORDER BY z DESC ')

    print( paste( i, r.iter, n.iter, system.time( {
					Top.Out[[i]] <- sqlQuery(conn, q.str)})[3] ))
  }
}
})

## Mappings: PredField Name ---------------------------------------------------

#TT Column Names
  qf.names<-sqlColumns(conn,'TT')['COLUMN_NAME']
  qf.names<-data.frame(cbind(ind=(1:(dim(qf.names)[1])),fields = qf.names))

#Import QF Mapping Table
  qf.tbl <- list(); qf.tbl[[10]] <- 0	
  tbl.vec <- c('holder', 'PlaceSvcTbl',  'SpecialtyTbl' ,'PCGTbl', 'ProcGTbl' )
  for (i in 2:5) {qf.tbl[[i]] <- sqlQuery( conn,
						 ps('SELECT * FROM',tbl.vec[i]) ) 	}

#PredName Mapping Func
  qf.lookup <- function( ... ) { 
	x <- as.numeric(...)[1]
 	 if (x == 0) '' else {
		y <- as.numeric(...)[2]
		 if (y == 0) '' else {
	if (!is.null(qf.tbl[[x]])) as.character(qf.tbl[[x]][y,4]) else
	paste( qf.names[x,2], '_', y, sep = '') 	}}			     } 

	  #Check its performace
	   #qf.lookup(c(2,2))
	   #xx<-matrix(1:6,nrow=3,ncol=2)
	   #apply(xx,1,qf.lookup)

#Map the field names for all cols in each row of each PT table
 Map.TopOut <- lapply( seq(along=Top.Out), function(x) {	
		
		pt.name <- PT.ind[[(PT.map[x,2])]][(PT.map[x,3]),] 
		tbl.dim <- dim(Top.Out[[x]])

		ptname.mat <- matrix(pt.name, nrow = tbl.dim[1],
					   ncol = (tbl.dim[2]-3), byrow = TRUE)

		cols.list <- lapply( seq(tbl.dim[2]-3), 
					   function(y, x_pass) {
						cbind(ptname.mat[,y],
							Top.Out[[x_pass]][,y]) },
					   x_pass = x )  

		ret.list<- lapply( seq(along=cols.list), 
			  function(z) {apply(cols.list[[z]],c(1), qf.lookup)} )

		ret.char <- sapply( ret.list, I, simplify = 'array')		

		apply(ret.char, 1, function(x) { paste(x,collapse=' ')})    })


## Build full.topout Display Mat ----------------------------------------------

  TopOut.length <- sapply(Top.Out,dim,simplify=T)
  ini.row <- cumsum( TopOut.length[1,] )

  Z.out<-lapply(seq(along=Top.Out),function(x) { 
  	          data.frame( cbind( 
				ind = (  ( 1+ max(0, ini.row[x-1])) : (ini.row[x]) ),
		         	y = Top.Out[[x]]$y, n = Top.Out[[x]]$n, 
				z = Top.Out[[x]]$z ) ) })
  Z.out <- do.call("rbind",Z.out)

  QF.out<-lapply(seq(along=Top.Out),function(x) { 
  	    data.frame( qfname = Map.TopOut[[x]] ) } )
  QF.out <- do.call("rbind",QF.out)

  rm(full.topout)
  full.topout <- data.frame(cbind(QF.out,Z.out))
  full.topout <-  full.topout[ order(full.topout$z, decreasing=T), ]
  row.names(full.topout) <- as.character(1:(dim(full.topout)[1]))
  
  options(digits = 3)
  full.topout
  dim(full.topout)
  fnname <- "C:\\Users\\Owner\\Desktop\\full_topout.csv"
  write.csv(full.topout, file=fnname, row.names = TRUE)

  #hist(ifelse(full.topout[,3]>10000,100000,full.topout[,3]),breaks=20)


## Selection Matrix -----------------------------------------------------------

  full.topout <-  full.topout[ order(full.topout$z, decreasing=T), ]
  
  selection.ind <- full.topout$ind[seq(min(nrow(full.topout),topsift.x))] 
  
  #take-out rejects
   #selection.ind <- setdiff(selection.ind, selection.ind[
	#				c(147,195,303,318,322,369)] )

## Build select.map: SQL-Query statements for each PredField -----------------
# Same algo as before, but we always just paste( qf.name[x], 'when', y)
 
   case.func <- function( ... ) { 
			x <- as.numeric(...)[1]
 	 		if (x == 0) '' else {
			  y <- as.numeric(...)[2]
		 	  if (y == 0) '' else {
			   paste('CASE', qf.names[x,2], 
				   'WHEN', y, 'THEN',
				    sep = ' ') 		 }} }

   else.func <- function( ... ) { 
			x <- as.numeric(...)[1]
 	 		if (x == 0) '' else {
			  y <- as.numeric(...)[2]
		 	  if (y == 0) '' else {
			   'ELSE 0 END'	 }} }
  

#List of Select Statements for each TopOut
 select.map <- lapply( seq(along=Top.Out), function(x) {	
		
		pt.name <- PT.ind[[(PT.map[x,2])]][(PT.map[x,3]),] 
		tbl.dim <- dim(Top.Out[[x]])
		ptname.mat <- matrix(pt.name, nrow = tbl.dim[1],
					   ncol = (tbl.dim[2]-3), byrow = TRUE)
		
		cols.list <- lapply( seq(tbl.dim[2]-3), 
					   function(y, x_pass) {
						cbind(ptname.mat[,y],
							Top.Out[[x_pass]][,y]) },
					   x_pass = x )  
		
		case.list<- lapply( seq(along=cols.list), 
			  function(z) {apply(cols.list[[z]],c(1), case.func)} )
		else.list<- lapply( seq(along=cols.list), 
			  function(z) {apply(cols.list[[z]],c(1), else.func)} )

		case.char <- sapply( case.list, I, simplify = 'array')		
		else.char <- sapply( else.list, I, simplify = 'array')		
		
		case.full <- apply(case.char,1,function(a1) {
							     paste(a1,collapse=' ')}  ) 
		else.full <- apply(else.char, 1, function(a2) {
				 			       paste(a2,collapse=' ')} )    
		
		sapply(seq(along=case.full), function(b) {
			paste(case.full[b], ' 1 ', else.full[x], sep='') } )
})


#Same as Map.TopOut just compress var names for colnames in dataframe
 MapDot.TopOut <- lapply( seq(along=Top.Out), function(x) {			
		pt.name <- PT.ind[[(PT.map[x,2])]][(PT.map[x,3]),] 
		tbl.dim <- dim(Top.Out[[x]])
		ptname.mat <- matrix(pt.name, nrow = tbl.dim[1],
					   ncol = (tbl.dim[2]-3), byrow = TRUE)
		cols.list <- lapply( seq(tbl.dim[2]-3), 
					   function(y, x_pass) {
						cbind(ptname.mat[,y],
							Top.Out[[x_pass]][,y]) },
					   x_pass = x )  
		ret.list<- lapply( seq(along=cols.list), 
			  function(z) {apply(cols.list[[z]],c(1), qf.lookup)} )
		ret.char <- sapply( ret.list, I, simplify = 'array')		
		apply(ret.char, 1, function(x) { paste(x,collapse='_')})    
})


#Now Add the Prefix and Suffix
  SQL.TopOut <-sapply(seq(along=select.map), function(x) {
			sapply( seq(along=select.map[[x]]), function(y,x_pass) {
				paste( MapDot.TopOut[[x_pass]][y],
					 ' = ', agg.func, '( ',
					select.map[[x_pass]][y] )  }, x_pass = x ) } )

 SQL.out<-do.call("rbind", lapply( seq(along=SQL.TopOut), function(x) { 
  	    matrix( SQL.TopOut[[x]] ) } ) )


 #Add a prefix to varname for its rank in topout z value
  Selection.SQL <- matrix(sapply( seq(along=selection.ind), function(x) {
				 paste( var.prefix, x, '_',
				SQL.out[selection.ind[x],1] , sep = '')}))

  dim(Selection.SQL)


## Execute The SQL statement to return PredFields to R-----------------------


 #Rebuild TT for all N
    check.drop('TT')
    q.str <- {'CREATE TABLE TT (  mapmy int, pst tinyint, spect tinyint,
                              pcgt tinyint, proct tinyint, lost tinyint,
					date1 tinyint, id1 smallint, id2 smallint,
					 )'}
    sqlQuery(conn, q.str)
    q.str <-{'INSERT INTO TT 
		  SELECT TTx.*
		  FROM TTx
		  '}
    sqlQuery( conn, q.str )



  Str.SQL <- apply( Selection.SQL, 2, paste, collapse = ' ), ')
  Str.SQL <- paste(' SELECT MapMY, ', Str.SQL, ' ) FROM TT GROUP BY MapMY ' )

  system.time( {
    sift.main[[sift.ind]] <- sqlQuery( conn, Str.SQL) 
  })

  #Put in same mapmy order as data.model
   library(plyr)
   sift.main[[sift.ind]] <- join( x = data.frame(MapMY = data.model$MapMY),
					    y = sift.main[[sift.ind]],
					    by = "MapMY",
					    type = "left" )                           
  #Check
   dim(sift.main[[sift.ind]])  
   sift.main[[sift.ind]][1:10,1:3]
  

## Select sift.ind from sift.main for next module -------------------------------
  

## CODA --------------------------------------------------------------------
#save.image("C:\\Users\\Owner\\Desktop\\HPN R Directory\\siftWS")







