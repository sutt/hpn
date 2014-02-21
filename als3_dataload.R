
#C:\Program Files\Microsoft SQL Server\MSSQL10_50.SQLEXPRESS\MSSQL\DATA

#Provider mapping file:
#c:\Program Files (x86)\Microsoft SQL Server\100\DTS\MappingFiles\SSIS10ToMSSQL.XML
setwd("C:/Users/Owner/Desktop/ALS1")
library(RODBC)
library(plyr)

##sql setup
c.str <- "driver=SQL Server;database=ALS1;server=Owner-PC\\SQLEXPRESS;"
conn <- odbcDriverConnect(c.str)

check.drop <- function(x) {
    db.tbls <- sqlQuery(conn,"SELECT * FROM 
                        sysobjects WHERE xtype = 'U' ")[,1]
    if ( x %in% db.tbls ) { sqlQuery(conn, paste('DROP TABLE ',x))
                            print('dropped') } }
 
##Optional but not optimal: RODBC -> SQL ------------------------------------

##import from file
traindata <- read.csv("train.txt", header=F, sep='|') 
nrow(traindata)
traindata[1:10,]
object.size(get('traindata')) / 1000000

check.drop('train_Rimport')
sqlSave(   conn
           ,dat = traindata
           ,tablename = 'train_Rimport'
           ,append = FALSE
           #,typeInfo = 'integer', 'integer', 'integer'
           ,varTypes = c( V1 = 'int', V2 = 'int', V3 = 'varchar(50)',
                          V4 = 'varchar(50)', V5 = 'int', 
                          V6 = 'varchar(50)', V7 = 'varchar(100)' )
           ,colnames = FALSE
           ,rownames = FALSE
           ,fast = TRUE						)

##build custom FullMeta train table-------------------------------------
check.drop('FullMeta')
q.str <- {'
    SELECT subjectID    = cast(V1 as int)  
            ,formID     = cast(V2 as int)  
            ,formName   = cast(V3 2] as varchar(21))
            ,jsonStr    = cast(V4 as varchar(36))
            ,fieldID    = cast(V5 as int)
            ,fieldName  = cast(V6 as varchar(44))
            ,val        = cast(V7 as varchar(55))		
    INTO FullMeta	  
    FROM dbo.train
'}; dummy <- sqlQuery(conn, q.str)

###Build Seperate From Tables -------------------------------------------

  q.str  <- {'SELECT Distinct(formID) as formID, formName 
              FROM FullMeta Order By formID DESC'}
  form.tbl <- sqlQuery(conn ,q.str, stringsAsFactors = FALSE )  

    #alter for INTO table name format
    tbl.name <- gsub(" ", "_", form.tbl$formName) 
    tbl.name[which(tbl.name=='ALSFRS(R)')] <- 'ALSFRSR'
    tbl.name

  ##find delta field for each table
   q.str  <- {'SELECT formID, formName, fieldID, fieldName FROM FullMeta
               GROUP BY formID, formName, fieldID, fieldName
               Order By formID DESC '}
   field.tbl <- sqlQuery(conn ,q.str, stringsAsFactors = FALSE )
   delta.ind <- grep("Delta",field.tbl$fieldName)  
   field.tbl[delta.ind,]


### Iterate over each table #####
for ( i.form in seq(length(tbl.name)) ) {
    
    i.form <- 9
   
    tbl.ind <- c('ALSFRSR','Laboratory_Data','Vital_Signs',
                'Forced_Vital_Capacity', 'Slow_Vital_Capacity',
                'Treatment_Group', 'Family_History', 'Demographics',
                'Subject_ALS_History')[i.form]

    formID.ind <- c(145,146,183,149,212,217,147,144,148)[i.form]
    
    print(i.form) 
    print(tbl.ind)
    

##Build Seperate tables for each formID ----------------------------------    
q.str <- {paste(" SELECT *, row_ind = Row_Number()
                                      Over (ORDER BY subjectID DESC)
                  INTO ",
                  tbl.ind,
                " FROM FullMeta WHERE formID = ", 
                  formID.ind
            , sep = " ") }

check.drop(tbl.ind)
print(system.time(dummy<-sqlQuery(conn, q.str)))

q.str <- paste( "Select Count(*), Max(row_ind) From ", tbl.ind)
print(sqlQuery(conn,q.str))
  
## Now build out each table in a PIVOT'd format --------------------

 #query for distinct fieldNames
    q.str <- paste( 'Select Distinct(fieldID), fieldName From ', tbl.ind)
    col_fields <- sqlQuery(conn, q.str)
    col_fields[order(col_fields[,1]),]      #printout colnames table
    col_str <- col_fields[order(col_fields[,1]),1]
    
    col_str <- paste(as.character(col_str), collapse = "] , [") 
    col_str <- paste("[", col_str, "]", sep = "")

  #build dynamic pivot query
  q.str <- {paste(
       'Select * INTO temp1 
        From ( select subjectID, ',
                col_str,
             ' From (Select jsonStr, subjectID, fieldID, row_ind  From ',
                tbl.ind,
                ' ) As p1 PIVOT 
                (
                    max(row_ind) For fieldID IN 
                    ( ',
                     col_str,
                    ' )
                 ) as p2
               ) as p3 ' ##Order By subjectID ASC, delta_ind ASC '
        ,sep =" " 
   ) }

  check.drop('temp1')
  sqlQuery(conn, q.str)
  sqlQuery(conn, 'Select Count(*) From temp1')

  #now map the val field still in varchar onto the table --------------------------
  col_str <- as.character(sqlColumns(conn, "temp1")[,4])
  col_str <- col_str[which(is.na(match(col_str, c('subjectID', 'delta_ind'))))]
    
  #identify delta field - needs to be cast as int for delta_ind to sort 
  delta_col  <- which(!is.na(match(col_str,(field.tbl[delta.ind,])$fieldID)))
  delta_sql <- paste( "[", col_str[delta_col[1]], "]" ,sep ="")
  
  #apply string formatting
  col_str <- as.character( sapply( col_str,  function(x) {
                           paste("[",x,"]",sep = "") }, simplify = TRUE ) )
  
  #apply NULLS where blank (text = '')
  sqlfunc_str <- as.character( sapply( col_str, function(x) { 
                                 paste( x, " = (Select 
                                        Case val When '' Then Null
                                        Else val End From ", 
                                        tbl.ind, 
                                  " m  Where m.row_ind = t.", x , " )"
                                , sep = "" ) }
                              , simplify = T ) )
  sqlfunc_str <- paste(sqlfunc_str, collapse = " , ")
    
 #build dynamic mapping query - with delta ranking index
  q.str <- { paste("
               Select p2.* Into temp2 From ( 
                   Select *, delta_ind = Dense_Rank() Over 
                                (Partition By subjectID Order By
                                (Case ", delta_sql, " When '' Then 9999
                                 Else Cast( ", delta_sql, " As int) End) ASC) 
                   From (Select subjectID, ",
                                sqlfunc_str,
                       " From temp1 t ) as p1
                   ) as p2 "
             , sep = " " ) }
                          
  check.drop('temp2')
  dummy <- sqlQuery(conn, q.str); sqlQuery(conn, 'Select Top 3 * From temp2')

#apply custom changes to field -------------------------------------------------
#add a new column for the type of ddl into which it will be transformed
#types - int, float, varchar(x)
  
    temp2.colnames <- as.character(sqlColumns(conn, "temp2")[,4])    

   #recall the field names attached to the ID's
    q.str <- paste('Select Distinct(fieldID), fieldName From ',tbl.ind)
    col_fields <- sqlQuery(conn, q.str)
    id_fields <- cbind( col_ind = 1+c(1:nrow(col_fields)), 
                        col_fields[order(col_fields[,1]),]    )
    id_fields[1:10,]

  #defaults
    user.title <- paste( 'var', seq(length(temp2.colnames)), sep = "")
    user.order <- seq(length(temp2.colnames))
    tbl_t.ddl <- rep('varchar(55)', length(temp2.colnames) )

  #Table-Specific
    switch( i.form
        ,user.title <- c('subject_id', 'd_ind', 'd_val',
                         's_total', 'r_total',
                         'ss1','ss2', 'ss3', 'ss4','ss5a', 'ss5b',
                         'ss6','ss7','ss8','ss9',
                         'ss10s','ss10r1','ss10r2','ss10r3')
            
      ,user.title <- c('subject_id', 'd_ind','d_val', 'TestName',
                       'TestResult', 'Unit')
            
      ,user.title <-c(  c( 'subject_id', 'd_ind', 'd_val') ,    
                        gsub(" ", "_", id_fields$fieldName[-c(4)]) )
      ,user.title <- c('subject_id', 'd_ind', 'd_val', 
                         'capacity', 'subjNorm', 'Unit')
      ,user.title <- c('subject_id', 'd_ind', 'd_val', 'capacity')            
      ,user.title <- c('subject_id', 'd_ind', 'd_val', 'StudyArm')            
    
      ,user.title <-c(  c( 'subject_id', 'd_ind', 'd_val') ,    
                             gsub(" ", "_", id_fields$fieldName[-c(1)]) )
       ,user.title <- c( 'subject_id', 'd_ind', 'd_val',
                         'Ethnicity', 'Sex','Asian','Black','Unknown',
                         'Caucasian','Age', 'RaceOther','RaceSpecify' )
       
        ,user.title <- c( 'subject_id', 'd_ind', 'd_val',
                          'AgeOnset','Symptom','SymptomSpecify',
                          'Location', 'SiteOnset', 'OnsetDelta',
                          'DiagnosisDelta')
            )
    
    switch( i.form 
        ,user.order <- c(1,19,13,14,15,2,4,5,6,7,8,9,10,11,12,3,16,17,18)
        ,user.order <- c(1,6,2,3,4,5)    
        ,user.order <- c(1,36,5,c(1:35)[-c(1,5)] )
        ,user.order <- c(1,6,4,2,3,5)            
        ,user.order <- c(1,4,3,2)
        ,user.order <- c(1,4,3,2)
        ,user.order <- c(1,29,2,c(1:28)[-c(1,2)] )
        ,user.order <- c(1,12,2,c(1:11)[-c(1,2)] )
        ,user.order <- c(1,10,2,c(1:9)[-c(1,2)] )
        )  
    
    switch( i.form
         ,tbl_t.ddl <- rep('int', length(temp2.colnames) )
         ,tbl_t.ddl[1:3] <- c('int','int','int')
         ,tbl_t.ddl[1:3] <- c('int','int','int')
         ,tbl_t.ddl[1:3] <- c('int','int','int')
         ,tbl_t.ddl[1:3] <- c('int','int','int')
         ,tbl_t.ddl[1:3] <- c('int','int','int')
         ,tbl_t.ddl[1:3] <- c('int','int','int')
         ,tbl_t.ddl[1:3] <- c('int','int','int')
         ,tbl_t.ddl[1:3] <- c('int','int','int')
    )

###Build Correct formatted Columns------------------------------------------
    lapply(seq(length(temp2.colnames)), function(x) { 
    
      #Add New Column    
        q.str <- paste( 'Alter Table temp2 Add ', user.title[x],
                        tbl_t.ddl[x], sep = " ");
        dummy <- sqlQuery(conn, q.str);
      
      #Update value from old column
        q.str <- paste( 'Update temp2 Set ', user.title[x],
                        ' = Cast( [', 
                            temp2.colnames[user.order[x]], 
                                 '] As ',
                                     tbl_t.ddl[x],
                                            ')'
                    ,sep = "");
         dummy <- sqlQuery(conn, q.str);  
        
      #Drop Old Column    
        q.str <- paste( 'Alter Table temp2 Drop Column [', 
                        temp2.colnames[user.order[x]], ']' 
                        ,sep = "");
        dummy <- sqlQuery(conn, q.str);
         
    })

  #Finally, a table to work with
   fmt.tbl <- paste(tbl.ind, "_t",sep="")

   q.str <- paste("Select * Into ", fmt.tbl, " From temp2
                   Order By subject_id ASC, 
                   (isnull(d_ind,99)) ASC",sep="")
   
   check.drop( fmt.tbl )
   dummy <- sqlQuery(conn, q.str)    


##Some Tables need More formatting ---------------------------------------
    
if (tbl.ind == 'Laboratory_Data') {
    
    check.drop('temp0L')
    sqlQuery(conn, 'Select *, row_ind = Row_Number() Over (Order By 
                      subject_ID DESC) Into temp0L FROM Laboratory_Data_t')

    q.str <- paste('Select Distinct(TestName) From temp0')
    col_fields <- sqlQuery(conn, q.str)
    col_str <- paste(as.character(col_fields[,1]), collapse = "] , [") 
    col_str <- paste("[", col_str, "]", sep = "")
    
    ##Build dynamic pivot-query1
    #Here's how it works: the pivot function is referencing the row_ind
    #    that the specific test is recorded on. From this temp1 table we can
    #    then lookup up the text field Result/Unit from this code num
    #    from the original temp0. This will occur next. Here, subject by Date
    #    index is "passed through" without needing to reference row_ind.
    
    q.str <- {paste(
        'Select * INTO temp1L
        From ( select subject_id, d_ind, d_val, ',
                col_str,
        ' From (Select subject_id, d_ind, d_val, row_ind, TestName 
                From temp0L ) As p1 
            PIVOT ( max(row_ind) For TestName IN 
                ( ',
                 col_str,
                ' )
             ) as p2
         ) as p3 ' ##Order By subjectID ASC, delta_ind ASC '
        ,sep =" " 
        ) }
    check.drop('temp1L')
    sqlQuery(conn, q.str)
    
   ##now map the val field still in varchar onto the table -----
    col_temp <- as.character(sqlColumns(conn, "temp1L")[,4])
    col_temp <- col_temp[which(is.na(match(col_temp, 
                            c('subject_id', 'd_ind','d_val'))))]
   
    #reference row number in subquery 
    #two element list for two cols: Results & Units
    sqlfunc_str <- list();  col.str <- list()
    for (col.ind in 1:2) {
    
    #apply string formatting
    var.prefix <- c("[","[Unit ")[col.ind]
        
    col.str[[col.ind]] <- as.character( sapply( col_temp,
        function(x) { paste(var.prefix, x,"]",sep = "") }, simplify = TRUE ) )
    
    #apply sql function syntax
    col.select <- c('TestResult','Unit')[col.ind]
    
    sqlfunc_str[[col.ind]] <- as.character( sapply( seq(length(col_temp)),
                function(x) { fn <- col.str[[col.ind]][x]
                              rn <- col.str[[1]][x]
                              paste( fn, " = (Case isNull( ", rn, ", 0) 
                                When 0 Then Null
                                Else (Select orig.", col.select, 
                              " From temp0L orig
                                Where orig.row_ind = t.", rn, ")
                        END)"
               , sep = "" ) }
        , simplify = T ) )
    
    sqlfunc_str[[col.ind]] <- paste(sqlfunc_str[[col.ind]], collapse = " , ")
    } ;    ##col.ind <-2;sqlfunc_str[[2]][1:5]
    
    #build 2nd dynamic mapping query - TestResults
    q.str <- { paste("Select p2.* Into temp2L From ( 
                        Select subject_ID, d_val, d_ind, ",
                            sqlfunc_str[[1]], " , ", sqlfunc_str[[2]],
                        " From temp1L t ) as p2 "
             , sep = " " ) }
    
    check.drop('temp2L')
    sqlQuery(conn, q.str)
    check.drop('Lab_t2')    
    sqlQuery(conn, 'Select *,date_t=isnull(d_val,0) Into Lab_t2 From temp2L')
   
    
}  #/end Lab custom-----------------------------------------------------------
        
if (tbl.ind == 'ALSFRSR') {
q.str <-{'Select *, 
           
           valid_visit = Case WHEN ( 
            ( (ss1 + ss2 + ss3 + ss4 + ss6 + ss7 + 
                ss8 + ss9) IS Not NULL)  AND
            ( (ss5a Is Not Null) or (ss5b Is Not Null) ) AND
            ( (ss10s Is Not Null) or (ss10r1 Is Not Null) )  AND
            (d_val Is Not NULL) ) THEN 1  ELSE 0 END,

            comp_Score = ss1 + ss2 + ss3 + ss4 + ss6 + ss7 +
              ss8 + ss9 + 
            (SELECT Max(v)  FROM (VALUES (ss5a), (ss5b)) 
                            AS value(v) ) +  
            (SELECT Max(v)  FROM (VALUES (ss10s), (ss10r1)) 
                                        AS value(v) ),             

            ss5 = (SELECT Max(v)  FROM (VALUES (ss5a), (ss5b)) 
                            AS value(v)),
            ss10 = (SELECT Max(v)  FROM (VALUES (ss10s), (ss10r1)) 
                                        AS value(v) ),
            
            ss_walking = ss8 + ss9,
            ss_kinetic = ss6 + ss7 + ss8 + ss9, 
            ss_finemotor = ss4 + isnull(ss5a,0) + isnull(ss5b,0) + ss6,
            ss_core = ss1 + ss2 + ss3+isnull(ss10s,0)+isnull(ss10r1,0),
            date_t = isnull(d_val,0)

        Into temp0a
        From ALSFRSR_t
        '}
    check.drop('temp0a'); sqlQuery(conn, q.str)
    check.drop('ALSFRSR_t')
    sqlQuery(conn,'Select * Into ALSFRSR_t From temp0a')
}

if (tbl.ind == 'Subject_ALS_History') {    
 
check.drop('temp1h')

sqlQuery(conn,"Select *, SymptomType = Case  isNull(SiteOnset, 'hey')
                                      When 'hey' THEN  
                                      (Row_Number() Over 
                                        (Partition By subject_id 
                                        Order By OnsetDelta))
                                      Else 1 END,

        SiteOnset =  ( Case SiteOnset When '1' Then 'Bulbar' 
                Else   Case SiteOnset When '3' Then 'Limb' 
                Else   Case SiteOnset When 'Onset: Bulbar' Then 'Bulbar'
                Else   Case SiteOnset When 'Onset: Limb' Then 'Limb'
                Else   Case SiteOnset When 'Onset: Limb and Bulbar' 
                        Then 'Both'
                Else  '' End End End End End )

               Into temp1h
               From Subject_ALS_History_t ")    

sqlQuery(conn,'Select Max(SymptomType) From temp1h')

check.drop('Subject_ALS_History_t')
sqlQuery(conn,'Select * Into Subject_ALS_History_t From temp1h')

}    
    
## Build Delta slope Calcs -----------------------------------------------
    
q.str <- {'Select subject_id, refvisit_dind = min(d_ind),
                                refvisit_dval = min(d_val)
            Into #temp_refvisit
            From ALSFRSR_t
            Where valid_visit = 1            
            Group By subject_id '}
    sqlQuery(conn,'Drop Table #temp_refvisit')
    sqlQuery(conn, q.str)

q.str <- {'Select subject_id,
                  refvisit_dind, 
                  first_dind = Min(d_ind), 
                  last_dind =  Max( Case When dif < 366 Then d_ind
                                         Else 0 End ) + 
                               Max( Case When dif >= 366 Then 1 Else 0 End)
            Into #temp_firstlast
            From (Select t.* , 
                         rv.refvisit_dind,
                         dif = (t.d_val - rv.refvisit_dval) 
                  From ALSFRSR_t t Left Join #temp_refvisit rv
                  On t.subject_id = rv.subject_id
                 ) p
            Where dif >= 92 
            Group By subject_id, refvisit_dind'}
    sqlQuery(conn,'Drop Table #temp_firstlast')
    sqlQuery(conn, q.str)

q.str <- {'Select a.subject_id,
           refvisit_score =  r.comp_score,
           first_score =  f.comp_score,
           last_score =  l.comp_score,
           refvist_days = isnull(r.d_val,0),
           first_days = isnull(f.d_val,0),
           last_days = isnull(l.d_val,0)
            Into SlopesMimic1
            From #temp_firstlast a
            Left Join ALSFRSR_t r On ( (a.subject_id = r.subject_id)
                                    And (a.refvisit_dind = r.d_ind) )
            Left Join ALSFRSR_t f On ( (a.subject_id = f.subject_id)
                                    And (a.first_dind = f.d_ind) )
            Left Join ALSFRSR_t l On ( (a.subject_id = l.subject_id)
                                    And (a.last_dind = l.d_ind) )
          '}
check.drop('SlopesMimic1')
sqlQuery(conn, q.str)

##import to R for ------------------------------
sm<- sqlQuery(conn,'Select * From SlopesMimic1')
dim(sm); names(sm)

sm$slope1 <- {  (365.24/12) * ( (sm$last_score - sm$first_score) / 
                                (sm$last_days - sm$first_days)      )}

subj2956<- sqlQuery(conn,'Select * From SlopesMimic1
                          Where subject_id =2956')
sm$slope1[2]

ground_truth <- read.csv("training_slopes.txt", header=F, sep="")
names(ground_truth) <- c('subject_id','slope_truth')
ground_truth[1:3,]

sm$slope_truth <- (join( x = sm, y = ground_truth, by = "subject_id",
                         type = "left" ))$slope_truth

sm$dif <- sm$slope1 - sm$slope_truth
mean(abs(sm$dif)) ; hist(sm$dif, 10); max(abs(sm$dif))
(sm[ which(abs(sm$dif) > .0000001 ),c(1,8:10)])

    
slope.calc <- sm[,c('subject_id','slope1')]
check.drop('slope_calc')
sqlSave(conn ,dat = slope.calc ,tablename = 'slope_calc'
        ,append = FALSE, varTypes = c( 'int', 'float' )
        ,colnames = FALSE, rownames = FALSE )

q.str <- {'select *, subject_index = row_number() over  
          (Order By slope1 DESC) Into slope_calc2 From slope_calc'}
check.drop('slope_calc2'); sqlQuery(conn,q.str)
    

frs.imp <- sqlQuery(conn, 'Select * From ALSFRSR_t')
#sqlQuery(conn, 'Select * From #temp_firstlast where subject_id = 341816')


##Cast Units Tables:  Vital Signs, Demo, Family, ALS_Hist ------------------
                    # Leave Lab open for tweaking
    i <- 4
    
    tbl.ind <- c('Lab_t2', 'Vital_Signs_t','Demographics_t',
                 'Subject_ALS_History_t')[i]
    
    tbl.colnames <- as.character(sqlColumns(conn, tbl.ind )[,4])
    work.col <- tbl.colnames[-c(1:3)]
    
    #Find non-numeric variation in results
    result.vary <- as.character( sapply( work.col, function(x) { 
        paste( "[",x, "] = Count(Distinct (Case IsNumeric([", x, 
               "]) WHEN 1 Then 'numeric' Else [", x, "] End) )"
       , sep = "") } 
    , simplify = T ))
    result.vary <- paste(result.vary, collapse = " , ")
    
    result.q <- sqlQuery(conn, paste('Select ', result.vary, 
                                       ' From ', tbl.ind, sep=""))
    
    cbind((1:length(result.q)), t(result.q))
    
    #if multiple disitnct, query those for the actual unit fields
    for (j in c(2,3,5)) {
        #length(work.col)){
        #j<-3
    str.distinct <- work.col[j]
    q.str <- paste( "Select Distinct [", str.distinct, "], ",
                            " Prev = Count(Distinct subject_id) ",
                     " From ", tbl.ind, 
                     " Group By [" , str.distinct,  "] "
                    ,sep = "")
    print(sqlQuery(conn,q.str))
    }
    #Find colnames identifed as units
    unit.ind <- grep("Unit", tbl.colnames) 
    unit.colnames <- tbl.colnames[unit.ind]
    result.colnames <- tbl.colnames[-c(unit.ind)]
    length(unit.colnames);length(result.colnames); min(unit.ind)
    
    #for lab can we match perfectly?
    unit.match <- sapply(result.colnames, function(x) { 
                    grep(x,unit.colnames)
                    }, simplify = T, USE.NAMES = T)
    
    #explore some fields for inclusion
    c.from <- 1
    c.to <- c.from + 14
    #c.from <- grep( "Uric", result.colnames); c.to <-c.from
    c.ind <- seq(from = c.from, to = c.to)
    
    print(result.colnames[c.ind], row.names = t)
    
    #how many unit fields are matches for each result metric
    unit.count <- sapply(c.ind, function(x) { cbind( 
                  result.colnames[x], length(unit.match[[x]])) }
                  ,simplify = TRUE)
    t(unit.count)
   
    #query table for distinct form of unit
    um.vec <- as.integer(unlist( unit.match[c.ind] ))
    unit.sql <- as.character( sapply( unit.colnames[um.vec], function(x) { 
            paste( "[",x, "] = Count(Distinct [", x, "] )", sep = "") }
            , simplify = T ))
    unit.sql <- paste(unit.sql, collapse = " , ")
    
    unit.count <- sqlQuery(conn, paste('Select ', unit.sql, 
                                       ' From ', tbl.ind, sep=""))
    cbind((1:length(unit.count)), t(unit.count))
    
    #if multiple disitnct units, query those for the actual unit fields
    unit.distinct <- unit.colnames[um.vec[2]]
    q.str <- paste( "Select Distinct [", unit.distinct, "] From ", tbl.ind
                    ,sep = "")
    sqlQuery(conn,q.str)

    #Vital Signs conversions
    conv.tbl <- data.frame(cbind(
                    UnitField = c('Height_Units','Weight_Units','SiteOnset')
                    ,Str1 = c('Inches','Pounds','1')   
                    ,Mult1 = c((1/12),1)
                    ,Str2 = c('Centimeters','Kilograms')
                    ,Mult2 = c( (1/(2.54*12)), 2.205 ) 
                    ,Str3 = c('CM','2')
                    ,Mult3 = c( (1/(2.54*12)), 2.205 )
    ))
    
    result.str.tbl <- data.frame(cbind(
                    UnitField = c('SiteOnset')
                    ,Str1 = c('1')
                    ,Mult1 = c('Bulbar')
                    ,Str2 = c('3')
                    ,Mult2 = c('Limb')
                    ,Str3 = c('Onset: Bulbar')
                    ,Mult3 = c('Bulbar')
                    ,Str4 = c('Onset: Limb')
                    ,Mult4 = c('Limb')
                    ,Str5 = c('Onset: Limb and Bulbar')
                    ,Mult5 = c('Both')
                    ))
    
    conv.fail.default <- "''"  #0
    unit.sw <- 1
    
    conv.func <- function(x){ if (x == '') x else { 
        
      tbl.work <- switch(unit.sw, result.str.tbl, conv.tbl)
      conv.work <- tbl.work[ which( tbl.work$UnitField == x),]                       
      how.many <-  length( which(!is.na(conv.work[-c(1)])) ) / 2
      
      paste.cust <- function(...) {paste(..., collapse = " ")}
      
       out <- do.call("paste.cust", 
            lapply( seq(how.many), function(y) { 
                 paste( 
                 " Case ", conv.work[1,1], " When '", conv.work[1,(2*y)],
                       "' Then ", conv.work[1,((2*y) + 1)], " Else "
                 ,sep = "")} ) )

      out2 <- paste( (if (unit.sw == 2) " * ( " else "(" ) , 
                     out, " ", conv.fail.default, " ", 
                     paste( rep("End", how.many), collapse = " "),
                   " )",
              sep = "" )                                    
      out2 }  }  #/end conv func
    

    #select fields to include ----------------------------------
    select1 <- c(4:11)    #(4:15)
    inp.cols <- result.colnames[select1]
    select1.str <- c(5)
    str.inp.cols <- lapply(inp.cols[select1.str], conv.func )
    
    #for units only-----------------
    select1.units <- c(1,4)   
    unit.inp.cols <- rep("",length(select1))
    unit.inp.cols[select1.units] <- unit.colnames[c(3,2)]
    conv.sql <- lapply(unit.inp.cols, function(x) {conv.func(x)} )
    
    user.title <- result.colnames[select1]
    title.sql <- as.character( sapply( user.title, function(x) { 
        paste( "[", x, "]",sep = "") }, simplify = TRUE ) )
    
    cast.sw <- 1  #2
    unit.needed.sw <- 1 #2
    
    sql.cast.func <- function(x) { 
        paste( title.sql[x], " = Cast( [" , inp.cols[x], "] as float)",
               conv.sql[[x]][1]
    
    
    #Cast Func: add string result handling later
    col.str <- as.character( sapply( seq(length(select1)), 
        ,sep = "") }
        ,simplify = TRUE ) )
    
    col.str <- paste(col.str,collapse = " , ")
    
    #output a proper table, now with the suffix "_x"
    q.str <-{ paste("Select subject_id, d_ind, d_val, ",
                    col.str,
                    " Into ", tbl.ind, "_x",
                    " From ", tbl.ind
                    ,sep ="") }
    
    check.drop(paste(tbl.ind,"_x",sep=""))
    sqlQuery(conn,q.str)

    
## Build Table Joining Functions -----------------------------------    

#1) Flat Table "bySubject alone" for non-multidelta Tables
    
       #index off of different tables convention eg History has multirows
       #per subject- do we need to max over RecordType or what
    
#2) a.Point-in Time value from a TimeTable
   #b.change (abs/pct) between two times
    
    point.fromTime <- function( inDate, inTbl, xtra) {
        
        d.style <- (if (length(inDate) == 1) 1 else 2)
        
        
        
        
    }
    
    
#3) Derive TimeTable (master date index -> "As of" value for OtherTbl)

    
#4) Lag operator? Delta abs/pct from other time [or save for R]
    
    
## Build Motion Chart DataTable --------------------------------------

q.str <-{"Select p.*, 
 
            slope3_12 = isnull(SC.slope1,0),
            subject_index = SC.subject_index

        Into ChartStart 
        From ALSFRSR_t p
        Left Join slope_calc2 SC On p.subject_id =  SC.subject_id
        "}
##isnull(ss10s,0) + isnull(ss10r1,0),
check.drop('ChartStart'); sqlQuery(conn, q.str)

q.str1 <-{'Alter Table ChartStart Add id_t varchar(255)'}
q.str2 <- {"Update ChartStart set id_t = cast(subject_index as varchar(3))  
            + 's_' + cast(abs(round(slope3_12 * 10,0)) as varchar(55)) "}
sqlQuery(conn,q.str1);sqlQuery(conn,q.str2)


q.str <- {"Select id_t, 

            base_time = (C.date_t - SM.first_days),
            
           ALSFRS_Total = comp_score, 
            s1_Speech = ss1,
            s2_Salivation = ss2,
            s3_Swalloing = ss3,
            s4_Handwriting = ss4,
            s5_Dining = ss5,
            s6_Dressing = ss6,
            s7_Bedding = ss7,
            s8_Walking = ss8,
            s9_Climbing = ss9,
            s10_Breathing = ss10,
            FRS_Mobile = ss_walking, 
            FRS_Kinetic = ss_kinetic,
            FRS_FineMotor = ss_finemotor,
            FRS_CoreFuncs = ss_core,

           in_trial = Case When date_t >= SM.refvist_days Then 
                         Case When date_t <= SM.last_days Then 
                            Case When date_t < 320 Then 2 Else 1 End
                         Else 0 End
                      Else 0 End,

            date_repeat = row_number() Over (Partition By
                C.id_t, date_t Order By comp_score DESC)
           
            Into ChartTable1
            From ChartStart C
            Left Join SlopesMimic1 SM 
            On C.subject_id = SM.subject_id
             Where C.Valid_visit = 1
          "}
check.drop('ChartTable1'); sqlQuery(conn, q.str)

    ####GENERALIZE! for two static points: delta_X
##add y-transform for dif from first days
q.str <- '\n
Select p.*,
        delta_ALSFRS_Total = p.ALSFRS_Total - q.ALSFRS_Total,
            delta_s1_Speech = p.s1_Speech - q.s1_Speech,
            delta_s2_Salivation = p.s2_Salivation - q.s2_Salivation,
            delta_s3_Swalloing = p.s3_Swalloing - q.s3_Swalloing,
            delta_s4_Handwriting = p.s4_Handwriting - q.s4_Handwriting,
            delta_s5_Dining = p.s5_Dining - q.s5_Dining,
            delta_s6_Dressing = p.s6_Dressing - q.s6_Dressing,
            delta_s7_Bedding = p.s7_Bedding - q.s7_Bedding,
            delta_s8_Walking = p.s8_Walking - q.s8_Walking,
            delta_s9_Climbing = p.s9_Climbing - q.s9_Climbing,
            delta_s10_Breathing = p.s10_Breathing - q.s10_Breathing,
            delta_FRS_Mobile = p.FRS_Mobile - q.FRS_Mobile,
            delta_FRS_Kinetic = p.FRS_Kinetic - q.FRS_Kinetic,
            delta_FRS_FineMotor = p.FRS_FineMotor - q.FRS_FineMotor,
            delta_FRS_CoreFuncs = p.FRS_CoreFuncs - q.FRS_CoreFuncs
Into ChartTable2
From ChartTable1 p
Left Join ( Select a.* From ChartTable1 a Where 
            ( (a.date_repeat = 1) And (a.base_time = 0) ) ) as q
On p.id_t = q.id_t
\n'
check.drop('ChartTable2'); sqlQuery(conn, q.str)

#import unique id x time
q.str <- {'Select * From ChartTable2 where ( (date_repeat = 1)
                And ( in_trial = 2))
           Order By base_time ASC '}
ct1 <- sqlQuery(conn, q.str )
dim(ct1);names(ct1)

#date adj
date_base <- as.Date(x="2000-01-01", format = "%Y-%m-%d")
ct1[,c('base_time')] <- date_base + ct1[,c('base_time')]

#random seed
rand_tbl<- data.frame(id_t = sqlQuery(conn,
           'Select Distinct id_t From ChartTable1', stringsAsFactors = F) ) 
rand_tbl$rand <- round( runif(n= nrow(rand_tbl),min=0, max=100), digits = 0)
rand_tbl[1:10,]; nr <- nrow(rand_tbl); nr
temp_cols <- join( x = ct1, y = rand_tbl, by = "id_t", type = "left" )

#build proto-chart table
num_subj <- 35
tbl.ind <- which( temp_cols$rand < (100 * num_subj / 918) )
chrt.tbl1 <- ct1[tbl.ind, ]
dim(chrt.tbl1); names(chrt.tbl1)

#chart
require(googleVis)   
C <- gvisMotionChart(chrt.tbl1, idvar="id_t", timevar="base_time",
                     date.format = "%Y%m%d",
                     options= list(
                                   height=520, width=1200,
                                   showHeader = TRUE,
                                   showSidePanel =FALSE,
                                   state = myStateSettings ) )
plot(C)

#"xZoomedDataMin":"1999-09-01", xZoomedDataMax":"2001-03-01",
myStateSettings <-'\n
{"yLambda":1,
 "yAxisOption":"19",
 "orderedByY":false,
 "yZoomedIn":true,
 "yZoomedDataMax":7, "yZoomedDataMin":-10,

 "xLambda":1,
 "time":"2000-01-01",
 "xAxisOption":"_TIME",
 "orderedByX":false,
 "xZoomedIn":false,

 "iconKeySettings":[],"colorOption":"_UNIQUE_COLOR",
 "duration":{"timeUnit":"D","multiplier":1},
 "dimensions":{"iconDimensions":["dim0"]},
 "showTrails":false,
 "iconType":"LINE",
 "uniColorForNonSelected":false,
 "nonSelectedAlpha":0,
 "sizeOption":"_UNISIZE",
 "playDuration":15000}
\n'

##trial######
'\n
{"iconType":"BUBBLE","yAxisOption":"4","time":"2010",
 "yZoomedDataMin":29946,"showTrails":false,
 "uniColorForNonSelected":false,
 "nonSelectedAlpha":0.4,
 "sizeOption":"5",
 "playDuration":20000,"yLambda":1,
 "xZoomedIn":false,"orderedByX":false,
 "duration":{"timeUnit":"Y","multiplier":1},
 "xZoomedDataMin":673,"yZoomedIn":false,"xAxisOption":"3",
 "colorOption":"2",
 "dimensions":{"iconDimensions":["dim0"]},
 "yZoomedDataMax":114260000,"xZoomedDataMax":1191600,
 "orderedByY":false,"xLambda":1,
 "iconKeySettings":[{"key":{"dim0":"DL"}},{"key":{"dim0":"WN"}}]}
\n'

##############################

qq <- rbind(Fruits, Fruits[1,])
qq[,2] <- (2009 - qq[,2])*100
qq[10,2] <- 300
names(qq)[2] <- "notyr"

myStateSettings <-'
{"xZoomedDataMin":1199145600000,"colorOption":"2",
"duration":{"timeUnit":"Y","multiplier":1},"yLambda":1,
"yAxisOption":"4","sizeOption":"_UNISIZE",
"iconKeySettings":[],"xLambda":1,"nonSelectedAlpha":0,
"xZoomedDataMax":1262304000000,"iconType":"LINE",
"dimensions":{"iconDimensions":["dim0"] }
}
'

B<- gvisMotionChart(Fruits, idvar="Fruit", timevar="Year",
                    options= list(gvis.editor ='please',
                                  state=myStateSettings))
plot(B)

demo(googleVis)
myStateSettings <-{'
{"xZoomedDataMin":1199145600000,"colorOption":"2",
"duration":{"timeUnit":"Y","multiplier":1},"yLambda":1,
"yAxisOption":"4","sizeOption":"_UNISIZE",
"iconKeySettings":[],"xLambda":1,"nonSelectedAlpha":0,
"xZoomedDataMax":1262304000000,"iconType":"LINE",
"dimensions":{"iconDimensions":["dim0"]},
"showTrails":false,"uniColorForNonSelected":false,
"xAxisOption":"_TIME","orderedByX":false,"playDuration":15000,
"xZoomedIn":false,"time":"2010","yZoomedDataMin":0,
"yZoomedIn":false,"orderedByY":false,"yZoomedDataMax":100}
'}


M <- gvisLineChart(df,"country", c("val1","val2"),
                   options=list(title="Hello World",
                                titleTextStyle="{color:'red',
                                  fontName:'Courier',fontSize:16}",
                  hAxis="{title:'Country', 
                          titleTextStyle:{color:'blue'}}",
                  series="[{targetAxisIndex: 0},
                           {targetAxisIndex:1}]",
                  vAxes="[{title:'val1'},{title:'val2'}]") ) 

print(M,'chart')












##make adjustments for gender as "1 male or 2 fem" and onset 3-limb 1-bulbar
##build sub_delta_tbl
#define valid fields
#valid.field.deltas <- data.frame( df = c(1267,1174,1190,1234,1225))                                 
#check.drop('valid_delta_fields')







