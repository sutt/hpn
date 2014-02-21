#########################################################################
#hpn8  - 7/12
########################################################################

##General Maintenance
memory.limit()
memory.size()
z <- sapply(ls(), function(x) object.size(get(x)))
z<-as.matrix(rev(sort(z)))
z[,1] <- (z[,1] / 1000000)
library(plyr)

##Import from SQL
library(RODBC) 
{conn <- odbcDriverConnect("driver=SQL Server;database=hpn2;
          server=Owner-PC\\SQLEXPRESS;")}
alldata <- sqlQuery(conn,"select * from dbo.hpn_full")

data.model = alldata[which(alldata$YearT < 4),]
data.score = alldata[which(alldata$YearT == 4),]
rm(alldata)

fit.ind <- list( which(data.model$YearT == 2) )
fit.ind[[2]] <- which(data.model$YearT == 3)
test.ind <- list(fit.ind[[2]])
test.ind[[2]] <- fit.ind[[1]]

# Holdout Sampling - test.ind
for (i in 3:6) {
pct.cv <- c(0, 0, .5, .5, .2, .2)
rows.datamodel <- dim(data.model)[1]; allrows <- 1:rows.datamodel
rand.sample <- sample(allrows, 
  		            size= as.integer(rows.datamodel*(1-pct.cv[i])),
			            replace = FALSE)
fit.ind[[i]] <- rand.sample
test.ind[[i]] <- allrows[ ifelse(is.na(match(x=allrows, 
                        table=rand.sample)), TRUE, FALSE)]
}
#####################################################################
rm(i,rand.sample,rows.datamodel,allrows)
#save.image("C:\\Users\\Owner\\Desktop\\HPN R Directory\\hpn4.ImportData.RData")
#load("C:\\Users\\Owner\\Desktop\\HPN R Directory\\hpn4.ImportData.RData")
# Classify and Add Indexes
#####################################################################


# Loop over pcg to build PCG.ind[[i]] & epbin_pcgi ------------------
orig_vars <- 236
custom_in <- function(a,b) { b %in% a }
name_vec <- c( paste("PCGmin", 1:4, sep=""), paste("PCGmax", 1:4, sep="") )
PCG_fields <- which( ! is.na( match( names(data.model), name_vec) ) )
PCG.ind <- list()
for (i in 1:46) {
PCG.ind[[i]] <- c(1:dim(data.model)[1])[ 
	apply( data.model[ ,PCG_fields], 1, custom_in, b = i)]
zeros <- matrix( 0, dim(data.model)[1], 1)
zeros[PCG.ind[[i]]] <- 1
data.model[ ,orig_vars + i ] <- zeros }
names(data.model)[(orig_vars+1:46)] <- paste("epbin_pcg", 1:46, sep="")
epbin_fields <- (orig_vars+1:46)


# Outcome Sample Flags - three of them -------------------------------
data.model$false_pos <- ifelse( data.model$dif1 > 0,1,0)
data.model$false_neg <- ifelse( ( ifelse(data.model$dif1 < 0, 1, 0) 
    * data.model$dihBin *(data.model$CTbin + data.model$SLbin) ) 
    + data.model[,epbin_fields[25]]  > 0, 1, 0)    #for pcgi=blank
data.model$false_either <- ifelse(
	(data.model$false_pos + data.model$false_neg) > 0, 1, 0)

name_vec <- c('false_pos','false_neg','false_either')   # 1,2,3
outcomeflags_fields <- which(!is.na(match(names(data.model),name_vec)))

# 1626, 4179, 5805, respectively - only 100 overlapping rows


# Special 'Prime' PCG fields ------------------------------------------
# to fill data.model prime fields or to build target.main

  #Initial Declares
	DIH_fields <- which( ! is.na( match( names(data.model), 
					paste("DIHsynth", 1:4, sep="")) ) )
	mmi_fields <- which( ! is.na( match( names(data.model), 
					paste("mmi", 1:4, sep="")) ) )
	CoMorbid_fields <- which( ! is.na( match( names(data.model), 
					paste("CoMorbid", 1:4, sep="")) ) )
	PST_fields <- which( ! is.na( match( names(data.model), 
					paste("PST", 1:4, sep="")) ) )

	custom_nafunc <- function(x,b) ifelse(is.na(x),b,x)
	custom_onesfunc <- function(x) ifelse(x==0,1,x)


  #Turnkey Loop Settings
    pcg_prime_inp <- c(28)	# either one field, to fill or a vector
    date_max <- 12
    target.insert.ind <- c(2)		# to match the outcome to target.main

    for (pcg_prime in pcg_prime_inp) {

  	# build episode indices: N x 4 mat's	
  	# left_ep finds leftmost epsisode, ep_ind finds all eps
	
	ep_ind <- data.model[,PCG_fields] == pcg_prime
	ep_ind <- apply(ep_ind, c(1,2), custom_nafunc, b = 0 )
	
	date_ind <- data.model[,mmi_fields] <= date_max
	date_ind <- apply(date_ind, c(1,2), custom_nafunc, b = 0 )

	ep_ind <- ep_ind * cbind(date_ind, date_ind)

	left_ep <- ep_ind %*% ((-1)*diag( c(1:4,1:4) ))
	left_ep <- as.matrix(apply(left_ep, c(1), min))
	left_ep <- cbind( (left_ep==-1),(left_ep==-2),
				(left_ep==-3),(left_ep==-4) )

	ep_ind <- ep_ind %*% diag(c(2,3,5,7,2,3,5,7))
	ep_ind <- apply(ep_ind, c(1,2), custom_onesfunc)
	ep_ind <- as.matrix(apply(ep_ind, c(1), prod))
	ep_ind <- 1 * cbind(  ((ep_ind %% 2)==0),((ep_ind %% 3)==0),
				    ((ep_ind %% 5)==0),((ep_ind %% 7)==0)   )

  # build fields for both left_ep and ALL eps with pcg = prime
	# dihall_prime = sum(all_eps)
	# mmiall_prime = max(all_eps)  
	# CoMorbidall_prime = or(all_eps)	
	# PSTall_prime = max(all_eps)
	# NumEps_prime = count(all_eps)

	temp_tbl <- data.model[,DIH_fields]
	temp_tbl <- apply(temp_tbl, c(1,2), custom_nafunc, b = 0)
	data.model$epBin_prime <- apply(left_ep,c(1),sum)
	data.model$dihBin_prime <- apply((1*((1*(ep_ind * temp_tbl))> 0))
						   ,c(1),max)
	data.model$dihleft_prime <- apply((left_ep * temp_tbl),c(1),sum)
	data.model$dihall_prime <- apply((ep_ind * temp_tbl), c(1), sum)

	temp_tbl <- data.model[,mmi_fields]
	temp_tbl <- apply(temp_tbl, c(1,2), custom_nafunc, b = 0)
	data.model$mmileft_prime <- apply((left_ep * temp_tbl), c(1), sum)
	data.model$mmiall_prime <- apply((ep_ind * temp_tbl), c(1), max)

	temp_tbl <- data.model[,CoMorbid_fields]
	temp_tbl <- apply(temp_tbl, c(1,2), custom_nafunc, b = 0)
	data.model$CoMorbidleft_prime <- apply((left_ep * temp_tbl), c(1), sum)
	data.model$CoMorbidall_prime <- apply((ep_ind * temp_tbl), c(1), max)

	temp_tbl <- data.model[,PST_fields]
	temp_tbl <- apply(temp_tbl, c(1,2), custom_nafunc, b = 0)
	data.model$PSTleft_prime <- apply((left_ep * temp_tbl), c(1), sum)
	data.model$PSTall_prime <- apply((ep_ind * temp_tbl), c(1), max)

	data.model$NumEps_prime <- apply(ep_ind, c(1), sum)


}

aaa<-data.model[PCG.ind[[28]],pcgprime_fields]

# Final Touch up Fields ------------------------------------------------

  data.model$dihlog <- log1p(data.model$dih)
  data.model$dihleft_primelog <- log1p(data.model$dihleft_prime)
  data.model$dihall_primelog <- log1p(data.model$dihall_prime)

  data.model$SexT_f <- factor( data.model$SexT, labels = c('u','m','f'),
							ordered = FALSE)
  data.model$AgeT_f <-factor( data.model$AgeT, levels = c(0:9),
      	  labels =c('0','5','15','25','35','45','55','65','75','85'),
							ordered = FALSE)

# DIHall_pcgi fields ---------------------------------------------------

col_pcgi <- dim(data.model)[2]

for (pcg_prime in 1:46) {
  
  print(pcg_prime)
  date_max <- 12

  col_pcgi <- col_pcgi + 1

  DIH_fields <- which( ! is.na( match( names(data.model), 
					paste("DIHsynth", 1:4, sep="")) ) )

  custom_nafunc <- function(x,b) ifelse(is.na(x),b,x)
  custom_onesfunc <- function(x) ifelse(x==0,1,x)

  # build episode indices: N x 4 mat's	
  # left_ep finds leftmost epsisode, ep_ind finds all eps
	
	ep_ind <- data.model[,PCG_fields] == pcg_prime
	ep_ind <- apply(ep_ind, c(1,2), custom_nafunc, b = 0 )
	
	date_ind <- data.model[,mmi_fields] <= date_max
	date_ind <- apply(date_ind, c(1,2), custom_nafunc, b = 0 )

	ep_ind <- ep_ind * cbind(date_ind, date_ind)

	left_ep <- ep_ind %*% ((-1)*diag( c(1:4,1:4) ))
	left_ep <- as.matrix(apply(left_ep, c(1), min))
	left_ep <- cbind( (left_ep==-1),(left_ep==-2),
				(left_ep==-3),(left_ep==-4) )

	ep_ind <- ep_ind %*% diag(c(2,3,5,7,2,3,5,7))
	ep_ind <- apply(ep_ind, c(1,2), custom_onesfunc)
	ep_ind <- as.matrix(apply(ep_ind, c(1), prod))
	ep_ind <- 1 * cbind(  ((ep_ind %% 2)==0),((ep_ind %% 3)==0),
				    ((ep_ind %% 5)==0),((ep_ind %% 7)==0)   )

  # Assignment to dihPCGi_fields
	temp_tbl <- data.model[,DIH_fields]
	temp_tbl <- apply(temp_tbl, c(1,2), custom_nafunc, b = 0)

	data.model[ ,col_pcgi] <- apply((ep_ind * temp_tbl), c(1), sum)
      print(pcg_prime)
}
    
    #Convert to dihlog
     dihpcgi_fields <- c(302:347) 
     data.model[,dihpcgi_fields] <- log1p(data.model[,dihpcgi_fields])


# Define (nested) ColIndx for the original + added fields --------------

	#By SQL table
	  model1_fields <- 1:8
	  match_fields <- 9:15
	  obe_fields <- 16:53
	  mm_fields <- 67:191	#does NOT include demographics
	  sift_fields <- 192:236

	#Added Fields
	  flag_fields <- 283:285
	  # see end for pcgprime

	#Demographics hierarchy
	  demo_fields1 <- 7:8
	  demo_fields2 <- c(63,66)
	  demo_fields3 <- c(54:62,64:65)
	  demo_fields4 <- c(300:301)

	#MArketMAkers subset - refrenced off mm_fields
	  aggclaim_mm <- 1:29
	  pcg_mm <- 30:75
	  sp_mm <- 76:88
	  pg_mm <- 89:106
	  ps_mm <- 107:115
	  drug_mm <- 116:125

	#Misc
	  map_fields <- 1:3
	  dih  <- which(names(data.model)=='dih')
	  dihBin <- which(names(data.model)=='dihBin')
	  dihlog <- which(names(data.model)=='dihlog')
  	  dih_fields <- cbind( dih, dihBin, dihlog)

	#PCGprime fields

        dihpcgi_fields <- c(302:347)	# corrected form 348 
 	
	  pcgprime_fields <- which(!is.na(match(names(data.model),	  

	        c(  'epBin_prime', 'dihBin_prime',     # 1,2
			'dihleft_prime', 'dihall_prime',   # 3,4
	     		'mmileft_prime', 'mmiall_prime',   # 5,6
			'CoMobidleft_prime', 'CoMorbidall_prime',   # 6,7
			'PSTleft_prime', 'PSTall_prime',   # 8,9
			'NumEps_prime',			     # 10
			'dihleft_primelog', 'dihall_primelog') )))  #11, 12

#############################################################################
rm(temp_tbl, left_ep, ep_ind, name_vec, i, zeros)

ws.str <- 'C:\\Users\\Owner\\Desktop\\HPN R Directory\\Gemini_Data.RData'
save.image(ws.str)
load(ws.str)
memory.size()	#1193

#############################################################################

###Fresh Run ------------------------------------------------------------

    #Define Preserved Objects    
    mod.obj <- list()
    mod.rel.infl <- list()
    best.iter <- rep(1,30)

    score.all <- matrix( 0, nrow = nrow(data.model), ncol = 30)

    target.main <- data.model[ , c( map_fields[3], dihlog, dihBin ) ]

    mod.descript <- matrix( 0 ,nrow = 30,
          ncol = 17, dimnames = list( c(), c( 'i.0', 'i.1','t.1', 's.1', 
						 'd.1', 'f.1', 'f.2', 'f.3', 
						 'g.1', 'g.2', 'g.3', 'g.4', 'g.5', 
						 'g.6', 'm.1', 'm.2', 'm.3' )        ))

   #Set Random Sampling
   #Note: test.ind[[1:2]] will remain as Y2, Y3

    for (i in 3:6) {
	pct.cv <- c(0, 0, .5, .5, .2, .2)
	rows.datamodel <- dim(data.model)[1]; allrows <- 1:rows.datamodel
	rand.sample <- sample(allrows, 
  		            size= as.integer(rows.datamodel*(1-pct.cv[i])),
			            replace = FALSE)
	fit.ind[[i]] <- rand.sample
	test.ind[[i]] <- allrows[ ifelse(is.na(match(x=allrows, 
                        table=rand.sample)), TRUE, FALSE)]
			}

###Resume Run ----------------------------------------------------------

  library(gbm) 
 
  #Column Selection Indices -------------------------------------
   BASE_FIELDS <- list()					#f.1
   BASE_FIELDS[[1]] <- c(1,2)              	#dummy for all
   BASE_FIELDS[[2]] <- c(1,2)				#dummy for none
   BASE_FIELDS[[3]] <- c(demo_fields3, demo_fields2, mm_fields)
   BASE_FIELDS[[4]] <- c(demo_fields4, mm_fields)
   BASE_FIELDS[[5]] <- c(demo_fields4)

   SIFT_FIELDS <- list()					#f.2
   SIFT_FIELDS[[1]] <- c(1,2)	
   SIFT_FIELDS[[2]] <- c(1,2)	
   SIFT_FIELDS[[3]] <- c(1:2)					
   SIFT_FIELDS[[4]] <- c(1:300)

   MOD_FIELDS <- list()						#f.3
   MOD_FIELDS[[1]] <- 1
   MOD_FIELDS[[2]] <- 1
   MOD_FIELDS[[3]] <- c(1:100)
   MOD_FIELDS[[4]] <- tryagain.index2 

  #GBM-Parameters - vectors -------------------------------
   GBM_OBS    <- c( 50, 40, 30, 20, 15, 5)		# g.1
   GBM_SHRINK <- c( .05, .025, .1, .01, .001, .0075)	# g.2
   GBM_DEPTH  <- c( 1:8 )					# g.3
   GBM_DIST   <- c( "gaussian", "bernoulli", 		# g.4
			  "adaboost", "poisson"   )
   GBM_TREES  <- c( 100, 400, 600, 900, 2500)		# g.5
   GBM_BAG    <- c( 1, .75, .5)				# g.6

   TARGET_IND <- c(1:5)						# t.1

 ##parameter-loops:
 
   mod.ind <- 14		#create model mod.ind+1

   i.0 <- (1:2)[2]	# Test Style:     1=holdout,  2=testrows at end
   i.1 <- (1:6)[5]      # fit/test ind
   d.1 <- (1:2)[1]	# Domain:		1=agg,      2=domain-specific
   s.1 <- (1:9)[1]	# SIFTER_IND      
   t.1 <- (1:9)[4]      # TARGET_IND      1=logdih,   2=dihBin, 3+ custom

 #m-loops: methods/misc 
   m.1 <- (1:2)[1]     # score response   	1 = response, 2= logit?
   m.2 <- (1:2)[1]     # trees to score   	1 = best.iter, 2 = all.trees
   m.3 <- (1:2)[2]     # best.iter method 	1 = OOB,  2 = test
   m.4 <- (1:2)[1]     # cbdind sift sw	      1 = sole sifttbl  2+ = multiple
   m.5 <- (1:2)[1]     # srun infl2tbl		1 = idle 	  2 = active

 #f-loops: select fields/columns			1=all, 2=none, 3+ custom
   f.1 <- (1:5)[5]  	# BASE_FIELDS  
   f.2 <- (1:4)[1]  	# SIFT_FIELDS  			
   f.3 <- (1:4)[1]  	# MOD_FIELDS

 #g-loops: gbm parameters
   g.1 <- (1:6)[1]     # GBM_OBS
   g.2 <- (1:6)[3]     # GBM_SHRINK
   g.3 <- (1:8)[4]     # GBM_DEPTH
   g.4 <- (1:4)[2]     # GBM_DIST
   g.5 <- (1:5)[2]     # GBM_TREES
   g.6 <- (1:3)[1]     # GBM_BAG (not used currently)

 #loop em if you got em --------
        
   #for (f.1 in c(5,4))  {	  


 ####Inside Loops: initiate model run --------------------------------
     
  mod.ind <- mod.ind + 1 ; print(mod.ind)

  ##sampling domain: d.1, i.1, t.1

    if (d.1 == 1)   samp.domain <- seq(nrow(data.model)) else {
   			  samp.domain <- which( (
						target.base[ , d.1]  |
						target.main[ , TARGET_IND[[t.1]] ]
							 ) == T )  			     }

    outsamp.domain <- (seq(nrow(data.model)))[-c(samp.domain)]
   
    fit.rows <- intersect(fit.ind[[i.1]], samp.domain )
    test.rows <- intersect(test.ind[[i.1]], samp.domain )
    

  ##mod.data build: combine data.model + sift.main(s)

     mod.data <- data.frame(MapMY = data.model$MapMY)

     base.fields <- switch( as.character(f.1), 
       '1' = seq(ncol(data.model)), '2' = NULL, BASE_FIELDS[[f.1]])

     sift.fields <- switch( as.character(f.2), 
       '1' = seq(ncol(sift.main[[s.1]][[1]])), '2' = NULL, SIFT_FIELDS[[f.2]])

     if (f.1 != 2)   mod.data <- cbind( mod.data,data.model[,base.temp] ) 

     if (f.2 != 2) { 
         if (m.4 == 1) { mod.data <- cbind( mod.data, 
				          (sift.main[[s.1]][,sift.temp])[,-c(1)] ) }    

         if (m.4 == 2) { dir.str <-  "C:/Users/Owner/Desktop/HPN R Directory/"
                         source( paste(dir.str,"sift.bind.func.R") )
                         mod.data <- cbind( mod.data, 
				                    sift.bind.func(c(1,2,3),"mod2") )}}

     if (f.3 > 1) mod.data <- mod.data[ , MOD_FIELDS[[f.3]] ] 	 
  

  ##Put into order and omit outsamp members from entering algo

    #i.0: order fit/test from samp domain  
    if (i.0 == 1)  train.order <- data.model$MapMY[fit.rows] else {
                   train.order <- data.frame( MapMY = data.model$MapMY[ 
							   c(fit.rows, test.rows)] )  }
    if (i.0 == 1) train.frac <- 1 else {
    train.frac <-round(length(fit.rows)/length(samp.domain),digits = 3) }
   			

    #t.1 mod.target col index
     mod.target <- join( x = train.order, 
	 			 y = target.main[ ,c(1,TARGET_IND[[t.1]])], 
		  	  	 by = "MapMY", 
				 type = "left" )[ ,2]

    mod.data.x <- join( x = train.order, 
			      y = mod.data, 
		  	      by = "MapMY", 
			      type = "left" )[ , -c(1) ]     #rm mapmy from col1

    print('going into gbm fit')


 ####GBM Method ---------------------------------------------------
  
  mod.obj[[mod.ind]]   <- gbm.fit(

             x = mod.data.x            
            ,y = mod.target

            ,distribution =  		GBM_DIST[[g.4]]
            ,n.trees =       		GBM_TREES[[g.5]]
            ,shrinkage =     		GBM_SHRINK[[g.2]]
            ,interaction.depth = 	GBM_DEPTH[[g.3]]
            ,n.minobsinnode =  	GBM_OBS[[g.1]]

	      ,keep.data = FALSE
	      ,train.fraction = train.frac
            ,verbose = TRUE) 

		# -------------------------------------------------------
  	        print('gbm fit done'); gc(reset=T); print(memory.size())


 ##Score algo on full set of members 
  
   #gbm.trees method 
   meth.tmp <- if (m.3 == 1) 'OOB' else 'test'
   best.iter[mod.ind] <- gbm.perf( mod.obj[[mod.ind]], method = meth.tmp)    
   gbm.trees <- if (m.2 == 1) best.iter[mod.ind] else GBM_TREES[[g.5]]

   score.out <- data.frame( MapMY = mod.data$MapMY ,   
 			          pred = predict.gbm( 
				      object = mod.obj[[mod.ind]], 
			            newdata =  mod.data[,-c(1)],     #rm mapmy
				      n.trees = gbm.trees, 
				      type = "response"            )  )	

   score.all[,mod.ind+1] <- join( x = data.frame(MapMY = data.model$MapMY), 
		   			    y = score.out,
	  				    by = "MapMY", 
					    type = "left"  )[,2]

   mod.rel.infl[[mod.ind]] <- summary.gbm(mod.obj[[mod.ind]])

  if (m.5 == 2) { dir.str <-  "C:/Users/Owner/Desktop/HPN R Directory/"
                  source( paste(dir.str,"infl2tbl.func.R") )
			infl2tbl.func( field.name = "mod2", blahblah)     }

 ##Record model param's

    mod.descript[mod.ind, ] <- cbind( i.0, i.1,t.1, s.1, d.1,
						  f.1, f.2, f.3, 
						  g.1, g.2, g.3, g.4, g.5, g.6,
						  m.1, m.2, m.3 )

   gc (reset=T); print(memory.size())

 ##Further Sub-Routines

dir.str <-  "C:/Users/Owner/Desktop/HPN R Directory/"
source( paste(dir.str,"trialsource2.R"))




}}
#}} }} }} }} }} }} }} }} }
#exit loops ----------------------------------------------------------


ws.str <- "C:\\Users\\Owner\\Desktop\\HPN R Directory\\Gemini_Model.RData"
save.image(ws.str)
load(ws.str)











#############################################################################

###Create aux.fields 
  
 #Existing Fields
 any.a <- as.matrix(apply(data.model[, sift_fields[c(4,9,13,32)] ],1,max))
 sum.a <- as.matrix(apply(data.model[, sift_fields[c(4,9,13,32)] ],1,sum))

 age.liberal <- as.matrix(apply(data.model[,c(63,55,56,57,58,59)], 1, max))
 age.conserv <- as.matrix(apply(data.model[,c(63,55,56,57)], 1, max))
 sex.check <- as.matrix(apply(data.model[,c(65,66)], 1, max))


 #Import Delivery Markers
  library(RODBC) 
  {conn <- odbcDriverConnect("driver=SQL Server;database=hpn2;
          server=Owner-PC\\SQLEXPRESS;")}
  import1 <- sqlQuery(conn,"select * from dbo.append7")

  #Put in same mapmy order as data.model
   mod.mapmy <- as.data.frame(data.model$MapMY)
   import1 <- merge( mod.mapmy, import1, by.x = "data.model$MapMY", 
     			   by.y = "mapmy", sort = FALSE )


  #Bind into Aux Fields
   aux.fields <- cbind( import1, any.a, sum.a, age.liberal, age.conserv,
				sex.check)

  #PregLogic1:
   score.all[,24] <-log1p(3.5)*(age.conserv & sex.check &
		   ( ifelse( (import1$SumLabPL + import1$SumLabSCS) >= 2, T, F) | 
		     import1$pred1a | import1$pred1b     )    &
	     (! ( (import1$delivery1a==1) | (import1$delivery2a==1) |
		    (import1$delivery1b == 1) | (import1$delivery3a==1) )) )


###Evaluation ##################


##Aggregate Eva

 # 147473/2 = 73736
  cust.func <- function(x) ((x^2)*73736)
  cust.func(.4635)

 #RMSLE decomposition
  rmsle_base <- sqrt(mean(data.model$dihlog^2))
  optc <- mean(data.model$dihlog)
  rmsle_optc <- sqrt(mean((optc-data.model$dihlog)^2))

 #Model Aggregrate: RMSLE, mean(dihlog) 
  rmsle.func <- function(mat,ind){ apply( 
				    (apply(mat, 1, "-", data.model$dihlog[ind])^2),
					2, mean) } 

  rmsle.func <-function(ind,mod){sqrt(mean((score.all[ind,mod] - 
						    data.model$dihlog[ind])^2))}


 options(digits = 4)
  for (x in c(1)) {print(rmsle.func(test.ind[[ mod.descript[x,1]]],x))}




##PCGi specific segment:


 mod.vec <- c(10,25:27)
 mod3 <- 21

 for (mod.ind in mod.vec) {

 i.1 <- 3 #mod.descript[mod.ind,1]			
 i.3 <- 2 #mod.descript[mod.ind,3]
 i.4 <- 1 #mod.descript[mod.ind,4]

 insamp.ind <- which(data.model$inSample == 1)
 outsamp.ind <- which(data.model$inSample == 0)
 fit.insamp <- intersect(insamp.ind, fit.ind[[i.1]])     
 test.insamp <- intersect(insamp.ind, test.ind[[i.1]])   
 test.all <- test.ind[[i.1]]			                	
 test.alloutsamp <- union( test.all, outsamp.ind )       

   if( i.3 == 1) {
     if( i.4 == 1) {rows1 <- test.all; y.ind <- dihlog }
     if( i.4 == 2) {rows1 <- test.all; y.ind <- dihBin } }

   if( i.3 == 2) {
     if( i.4 == 1) {rows1 <- test.insamp; y.ind <- dihpcgi_fields[i]
      rows2 <- intersect(PCG.ind[[i]],test.insamp); y2.ind <- dihlog  
	rows3 <- intersect(test.all, which(score.all[,mod3]>0))        }
     if( i.4 == 2) {rows1 <- test.insamp; y.ind <- epbin_fields[i] 
	rows2 <- intersect(PCG.ind[[i]],test.insamp); y2.ind <- dihBin  } }   

 options(digits = 1)

 if( i.4 == 1) {

 #SLE-full: for all rows1
  print(  sum(data.model[rows1,y.ind]^2) )
  
 #Model SLE:
  print(  sum( (  ifelse( score.all[rows1, mod.ind] > 0,
				  score.all[rows1, mod.ind], 0 ) 
		  - data.model[rows1, y.ind])^2 ) )

 #SLE-full: for all rows1
  print(  sum(data.model[rows2,y2.ind]^2) )
  
 #Model SLE:
  print(  sum( (  ifelse( score.all[rows2, mod.ind] > 0,
				  score.all[rows2, mod.ind], 0 ) 
		  - data.model[rows2, y2.ind])^2 ) )
 

 #SLE-full: for all rows1
  print(  sum(data.model[rows3,y2.ind]^2) )
  
 #Model SLE:
  print(  sum( (  ifelse( score.all[rows3, mod.ind] > 0,
				  score.all[rows3, mod.ind], 0 ) 
		  - data.model[rows3, y2.ind])^2 ) )
 }
 if( i.4 == 2) {}}




##Ensemble


#PregLogic1 - Loop to create increasingly stringent condition

  for (lab in 4:2) {
	#WRONG? add lab to score col index
   score.all[,21] <- score.all[,21] + (age.conserv & sex.check &

	     ( ifelse( (import1$SumLabPL + import1$SumLabSCS) >= lab, T, F) | 
		     import1$pred1a | import1$pred1b     )    &

	     (! ( (import1$delivery1a==1) | (import1$delivery2a==1) |
		    (import1$delivery1b == 1) | (import1$delivery3a==1) )) )  }


score.all[ ,14] <- score.all[ ,4]
score.all[ which(score.all[ ,22] > 0) ,14] <- score.all[

score.all[ ,15] <- apply( cbind( score.all[ ,4], score.all[ ,7]), c(1), max)

scaling.fact <- 1
score.all[ ,16] <- score.all[ ,4]
score.all[ which(score.all[ ,12] > 0) ,16] <- scaling.fact * 
          score.all[ which(score.all[ ,12] > 0) ,7]

score.all[ ,18] <- score.all[ ,4]
score.all[ which(score.all[ ,17] > 0) ,18] <- log1p(3.5)



##Fit Severity onto Classifiers

 #PregLogic 1-3
 i.1 <- 3
 mod <- 21
 
 insamp.ind <- which(data.model$inSample == 1))  
 pl.thresh.ind <- which(score.all[,mod] > 0)
 rows1 <- intersect(fit.ind[[i.1]], pl.thresh.ind)  
 rows2 <- intersect(test.ind[[i.1]], pl.thresh.ind)
# rows2 <- intersect( rows2, insamp.ind)

 sev.data <- matrix( cbind( data.model[ , dihlog ], score.all[,mod]),
 	  nrow = dim(data.model)[1], ncol = 2,dimnames = list( 
	  c(1:(dim(data.model)[1])), c("dihlog", "preg.logic1")))

 sev.data <- as.data.frame(sev.data)

 sev.data$preg.logic1 <- factor(  sev.data$preg.logic1, labels = 
				c('none','two','three','four'),ordered = FALSE)


 sev.mod.obj      <- lm( formula = dihlog ~ preg.logic1, 
   	                 family  = gaussian,
 		           data    = sev.data[rows2, ],
 		           method  = "glm.fit" )

 score.all[pl.thresh.ind,23] <- predict.lm( object = sev.mod.obj,
		       newdata = sev.data[pl.thresh.ind,], se.fit = FALSE)

 summary(sev.mod.obj)

mean(data.model[intersect(which(score.all[,21] == 1),rows2),dihlog])

max(score.all[,21])


mod<-21
for (x in 1:3) { score.all[,24 + x] <- score.all[,10]
		     score.all[ which(score.all[,21+x] > 0 ), 24 + x] <- 
		       (ifelse( score.all[,10] >  score.all[,21+x],
		               score.all[,10], 1*score.all[,21+x]    ))[
				  which(score.all[,21+x] > 0) ] }





##Evaluate PCGi SLE reduction to aggregate

#for (iter in 1:2) {

  i <- 28
  i.1 <- 3			#2+ iter
  mod.agg <- 10		#iter
  mod.pcgi <- 22	      # establishes both score and threshold right now
  scaling.fact <- 1

  rows1 <- test.ind[[i.1]]
  rows1.thresh <- intersect(which(score.all[,mod.pcgi] > 0), rows1)
  rows1.nothresh <- intersect(which(score.all[,mod.pcgi] == 0), rows1)

  insamp.ind <- which(data.model$inSample == 1)
  rows1.insamp <-intersect(insamp.ind, rows1)
  rows1.pcgi <- intersect(which(data.model[,epbin_fields[i]] == 1), rows1)
  rows1.pcgi.thresh <- intersect(which(score.all[,mod.pcgi] > 0), 
				  rows1.pcgi)
  rows1.pcgi.nothresh <- intersect(which(score.all[,mod.pcgi] == 0),
				  rows1.pcgi)

  # rows1 is for test.all (fullsample) dihlog, 
  # rows1.pcgi is for insample only

 #Baseline-GBM insample, SLE potential:
  sum((score.all[rows1.insamp,mod.agg]-data.model[rows1.insamp,dihlog])^2 )

 #Baseline-GBM insample PCGi,  SLE potential:
  sum( (score.all[rows1.pcgi,mod.agg] - data.model[rows1.pcgi,dihlog])^2 )

 #Baseline-GBM insample PCGi out-thresh,  SLE total:
  sum( (score.all[rows1.pcgi.nothresh, mod.agg] - 
	  data.model[rows1.pcgi.nothresh, dihlog])^2 )

 #Baseline-GBM insample PCGi in-thresh,  SLE potential:
  sum( (score.all[rows1.pcgi.thresh, mod.agg] - 
	  data.model[rows1.pcgi.thresh, dihlog])^2 )

 #Thresholded Indv-PCGi model score, SLE low point:
  sum(  ( (scaling.fact * score.all[ rows1.pcgi.thresh, mod.pcgi] ) - 
        	  data.model[rows1.pcgi.thresh,dihlog] )^2 ) 

 # Repeat the above for fullsample SLE...

#}


##ROC - PCGi

library(ROCR)

for (mod.ind in 13:20) {

   #Establish left-of-threshold-vector
    thresh.ind <- which(score.all[test.rows,mod] >= score.cutoff)

    thresh.score <- (score.all[test.rows,mod.ind])[thresh.ind]
    thresh.score <- (score.all[test.rows,mod])[thresh.ind]


    class.score <- (class.y[test.rows])[thresh.ind]		

    pred.list <- prediction( thresh.score, class.score)
    print( performance(pred.list, measure = "tpr", 
                                x.measure = "fpr")  	



##Derive Most-Needed-Conditions MAT
 
 i.1 <- 3
 mod <- 1
 mod.pcgi <- 9

 y.resid <- as.matrix(data.model[ , dihlog] - score.all[ , mod])
 floor.func <- function(x) { max(x,0) }
 ceiling.func <- function(x) { min(x,0) }
 y.resid1 <- apply(y.resid, c(1), ceiling.func)
 y.resid <- apply(y.resid, c(1), floor.func)
 y.resid1 <- y.resid1^2
 y.resid <- y.resid^2

 rows1 <-  test.ind[[i.1]]
 SLE.test <- sum( y.resid[rows1] ) 
 SLE.over.test <- sum( y.resid1[rows1] ) 


 rows1 <-  intersect(test.ind[[i.1]], insamp.ind)
 SLE.insamp <- sum( y.resid[rows1] )
 SLE.outsamp <- SLE.test - SLE.insamp

 rows1 <- intersect( which( data.model$Synthd == 0 ), rows1 )
 SLE.nooutcome <- sum( y.resid[rows1]  )

 x.mat <- data.model[ , c(demo_fields2,demo_fields3, epbin_fields)]
 x.mat <- cbind( x.mat, score.all[ , mod.pcgi] )
 mult.func <- function(x) { y.resid * x}
 xy.mat <- apply(x.mat, c(2), mult.func)

 rows1 <-  intersect(test.ind[[i.1]], insamp.ind)
 rows1 <- intersect( which( data.model$Synthd > 0 ), rows1 ) 
 SLE.bypcgi <- apply( xy.mat[rows1,], c(2), sum)
 SLE.outcome <- sum(SLE.bypcgi[-c(1:13,60)])
 SLE.overlap <- SLE.outcome - (SLE.insamp - SLE.nooutcome)

options(digits = 1)
 SLE.over.test
 SLE.test
 SLE.outsamp
 SLE.insamp
 SLE.nooutcome
 SLE.outcome
 SLE.overlap

 SLE.bypcgi

 output1 <- cbind(SLE.bypcgi, apply(x.mat[rows1,], c(2), sum) )

options(digits = 4)
 sum(SLE.outcome)/SLE.test

fnname <- "C:\\Users\\Owner\\Desktop\\SLEbyPCGi.csv"
write.csv(output1, file=fnname, row.names = TRUE)












