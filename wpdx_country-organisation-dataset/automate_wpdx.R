library(readr)

wpdx <- read_delim("C:/Users/Felix Knipschild/Dropbox/Network analysis/Water_Point_Data_Exchange_Complete_Dataset.csv", 
    "\t", escape_double = FALSE, trim_ws = TRUE)

#countries <- data.frame(table(wpdx$`#country_name`))
#countries$name <- wpdx$`#country_name`
#countries$id <- 
#countries <- aggregate(wpdx$`#country_name`, by=list(wpdx$`#country_name`), length)

countries <- aggregate(data.frame(record_count = wpdx[,c('#country_name')]), 
                       by=list(
                         wpdx$`#country_name`,
                         wpdx$`#country_id`
                         ), length)
countries$cntry_id <- as.numeric(row.names(countries))

n <- max(countries$cntry_id)

organisations <- aggregate(data.frame(records = wpdx$`#source`), by=list(wpdx$`#source`), length)

#SKIPPING BECAUSE OF JUNK DATA --> NEED TO COMBINE AUTOMATICALLY USING SIMILARITY...
#installer <- aggregate(data.frame(records = wpdx$`#installer`), by=list(wpdx$`#installer`), length)
#c(organisations$Group.1,installer$Group.1)

organisations$org_id <- as.numeric(row.names(organisations))+n

wpdx$data_lnk = ""
wpdx[!is.na(wpdx$`#data_lnk`),]$data_lnk <- wpdx[!is.na(wpdx$`#data_lnk`),]$`#data_lnk`

wpdx$orig_lnk = ""
wpdx[!is.na(wpdx$`#orig_lnk`),]$orig_lnk <- wpdx[!is.na(wpdx$`#orig_lnk`),]$`#orig_lnk`

datasets <- aggregate(data.frame(records = wpdx[,c("data_lnk","orig_lnk")]), 
                      by=list(wpdx$data_lnk, wpdx$orig_lnk), FUN = function(x,y) {1})
n <- max(organisations$org_id)
datasets$dataset_id <- as.numeric(row.names(datasets))+n

wpdx_m <- merge(x=wpdx, y=countries[,c('Group.2','cntry_id')], by.x = '#country_id', by.y = 'Group.2', all.x = TRUE)
wpdx_m <- merge(x=wpdx_m, y=organisations[,c('Group.1','org_id')], by.x = '#source', by.y = 'Group.1', all.x = TRUE)
wpdx_m <- merge(x=wpdx_m, y=datasets[,c('Group.1','Group.2','dataset_id')], by.x = c('data_lnk','orig_lnk'), by.y = c('Group.1','Group.2'), all.x = TRUE)

country_organisation <- aggregate(data.frame(records = wpdx_m[,c("cntry_id","org_id")]), by=list(wpdx_m$cntry_id, wpdx_m$org_id), length)
country_dataset <- aggregate(data.frame(records = wpdx_m[,c("cntry_id","dataset_id")]), by=list(wpdx_m$cntry_id, wpdx_m$dataset_id), length)
dataset_organisation <- aggregate(data.frame(records = wpdx_m[,c("dataset_id","org_id")]), by=list(wpdx_m$dataset_id, wpdx_m$org_id), length)

edges <- data.frame(`source` = c(country_organisation$Group.1, country_dataset$Group.1, dataset_organisation$Group.1), target = c(country_organisation$Group.2, country_dataset$Group.2, dataset_organisation$Group.2))

nodes <- data.frame(
  ID = c(countries$cntry_id, organisations$org_id, datasets$dataset_id), 
  Label = c(countries$Group.1,organisations$Group.1,paste("Dataset ", datasets$dataset_id, datasets$Group.1, datasets$Group.2," - ")), 
  Type=c(
    replicate(length(countries$cntry_id),"Country"),
    replicate(length(organisations$org_id),"Organisation"),
    replicate(length(datasets$dataset_id),"Dataset")
    )
  )

write.csv(edges,file = "edges.csv", row.names = FALSE)
write.csv(nodes,file = "nodes.csv", row.names = FALSE)
