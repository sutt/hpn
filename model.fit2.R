### Model.Fit #############################################################


#dir.str <-  "C:/Users/Owner/Desktop/HPN R Directory/"
#source( paste(dir.str,"trialsource2.R"))


###Fresh Run ------------------------------------------------------------

if (fresh.mod.sw == 2) {  print('fresh run initialized')

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
						  #m.4, m.5

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
}
###Resume Run ----------------------------------------------------------

  library(gbm); library(plyr) 
 
  #Column Selection Indices -------------------------------------
   BASE_FIELDS <- list()				        	#f.1
   BASE_FIELDS[[1]] <- c(1,2)                     	#dummy for all
   BASE_FIELDS[[2]] <- c(1,2)				        #dummy for none
   BASE_FIELDS[[3]] <- c(1,2)               #dummy for infltbl
   BASE_FIELDS[[4]] <- c(demo_fields3, demo_fields2, mm_fields)
   BASE_FIELDS[[5]] <- c(demo_fields4, mm_fields)
   BASE_FIELDS[[6]] <- c(demo_fields4)

   SIFT_FIELDS <- list()				        	#f.2
   SIFT_FIELDS[[1]] <- c(1,2)	
   SIFT_FIELDS[[2]] <- c(1,2)	
   SIFT_FIELDS[[3]] <- c(1:2)					
   SIFT_FIELDS[[4]] <- c(1:300)

   MOD_FIELDS <- list()					        	#f.3
   MOD_FIELDS[[1]] <- 1
   MOD_FIELDS[[2]] <- 1
   MOD_FIELDS[[3]] <- c(1:100)
   MOD_FIELDS[[4]] <- tryagain.index2 

  #GBM-Parameters - vectors -------------------------------
   GBM_OBS    <- c( 50, 40, 30, 20, 15, 5)	    	# g.1
   GBM_SHRINK <- c( .05, .025, .1, .01, .001, .0075)# g.2
   GBM_DEPTH  <- c( 1:8 )				        	# g.3
   GBM_DIST   <- c( "gaussian", "bernoulli", 		# g.4
			        "adaboost", "poisson"   )
   GBM_TREES  <- c( 100, 400, 600, 900, 1500)		# g.5
   GBM_BAG    <- c( 1, .75, .5)			        	# g.6

   TARGET_IND <- c(1:5)					        	# t.1

 ##parameter-loops:
 
   mod.ind <- 17		#create model mod.ind+1

   i.0 <- (1:2)[2]	  # Test Style:     1=holdout,  2=testrows at end
   i.1 <- (1:6)[5]    # fit/test ind
   d.1 <- (1:2)[1]	  # Domain:		      1=agg,      2=domain-specific
   s.1 <- (1:9)[1]	  # SIFTER_IND      
   t.1 <- (1:9)[2]    # TARGET_IND      1=logdih,   2=dihBin, 3+ custom

 #m-loops: methods/misc 
   m.1 <- (1:2)[1]     # score response   	1 = response, 2= link
   m.2 <- (1:2)[1]     # trees to score   	1 = best.iter, 2 = all.trees
   m.3 <- (1:2)[2]     # best.iter method 	1 = OOB,  2 = test
   m.4 <- (1:2)[1]     # cbdind sift sw	    1 = sole sifttbl  2+ = multiple
   m.5 <- (1:2)[1]     # srun infl2tbl		  1 = idle 	  2 = active

 #f-loops: select fields/columns			1=all, 2=none, 3+ custom
   f.1 <- (1:6)[4]  	# BASE_FIELDS  
   f.2 <- (1:4)[1]  	# SIFT_FIELDS  			
   f.3 <- (1:4)[1]  	# MOD_FIELDS

 #g-loops: gbm parameters
   g.1 <- (1:6)[3]     # GBM_OBS
   g.2 <- (1:6)[2]     # GBM_SHRINK
   g.3 <- (1:8)[4]     # GBM_DEPTH
   g.4 <- (1:4)[1]     # GBM_DIST
   g.5 <- (1:5)[5]     # GBM_TREES
   g.6 <- (1:3)[1]     # GBM_BAG (not used currently)

 #loop em if you got em --------
        
  # for (g.4 in c(1,2))  {	  
     
   #  t.1 <- c(2,3)[g.4]

    #mod.ind <- mod.ind + 1     #use for forced mod.ind
    print(mod.ind)

 ####Inside Loops: initiate model run --------------------------------

  ##sampling domain: d.1, i.1, t.1

    if (d.1 == 1)   samp.domain <- seq(nrow(data.model)) else {
   			        samp.domain <- which( (
						target.base[ , d.1]  |
						target.main[ , TARGET_IND[t.1] ]
							 ) == T )  			     }

    outsamp.domain <- (seq(nrow(data.model)))[-c(samp.domain)]
   
    fit.rows <- intersect(fit.ind[[i.1]], samp.domain )
    test.rows <- intersect(test.ind[[i.1]], samp.domain )
    

  ##mod.data build: combine data.model + sift.main(s)

     mod.data <- data.frame(MapMY = data.model$MapMY)

     base.temp <- switch( as.character(f.1), 
       '1' = seq(ncol(data.model)),
       '2' = NULL,
       '3' = c( 1, ( 1 + tbl2vars(field.name ="mod1_scr01", 
                                  base.sw = 1, sift.sw = 0) ) ) ,
       BASE_FIELDS[[f.1]] )

     sift.temp <- switch( as.character(f.2), 
       '1' = seq(ncol(sift.main[[s.1]])), 
	   '2' = NULL, 
	   '3' = c( 1, ( 1 + tbl2vars.func(field.name = "mod2", 
				                       base.sw = 0, sift.sw = 1) ) ),
       SIFT_FIELDS[[f.2]]  )

     if (f.1 != 2)  mod.data <- cbind( mod.data,data.model[,base.temp] ) 

     if (f.2 != 2) { 
         if (m.4 == 1) { mod.data <- cbind( mod.data, 
				                    sift.main[[s.1]][,sift.temp] ) }   
         if (m.4 == 2) { source("sift.bind.func.R")
                         mod.data <- cbind( mod.data, 
				                             sift.bind.func( c(1,2,3),"mod2")) }
     }

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
	 			                 y = target.main[ ,c(1,TARGET_IND[t.1]) ], 
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
            ,interaction.depth =GBM_DEPTH[[g.3]]
            ,n.minobsinnode =  	GBM_OBS[[g.1]]

	          ,keep.data = FALSE
	           ,train.fraction = train.frac
            ,verbose = TRUE) 

		# -------------------------------------------------------
  	        print('gbm fit done'); gc(reset=T); print(memory.size())


 ##Score algo on full set of members 
  
   #gbm.trees method 
   meth.tmp <- if (m.3 == 1) 'OOB' else 'test'
   best.iter[mod.ind] <- gbm.perf( mod.obj[[mod.ind]], method = meth.tmp,
                                   plot.it = F)    
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

   mod.rel.infl[[mod.ind]] <- summary.gbm(mod.obj[[mod.ind]], plotit=F)

   if (m.5 == 2) { source("infl2tbl.func.R") 
			             infl2tbl.func( field.name = "july", 
                                  rows.cut = 200, score.cut = .01, 
                                  base.sw = 1, sift.sw = 1)        }

 ##Record model param's

    mod.descript[mod.ind, ] <- cbind( i.0, i.1,t.1, s.1, d.1,
						  f.1, f.2, f.3, 
						  g.1, g.2, g.3, g.4, g.5, g.6,
						  m.1, m.2, m.3 )

   gc (reset=T); print(memory.size())

 ##Further Sub-Routines


}
#}} }} }} }} }} }} }} }} }
#exit loops ----------------------------------------------------------


ws.str <- "C:\\Users\\Owner\\Desktop\\HPN R Directory\\Gemini_Model.RData"
save.image(ws.str)
#load(ws.str)



#############################################################################
