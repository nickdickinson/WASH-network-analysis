source("shared_functions.R")

n_nodes <- 0

wpdx <- loadWPDX()
iati <- loadIATI()

countries <- loadCountries(0)

n_nodes <- max(countries$node_id)

# #source #installer
organisationsWPDX <- aggregate(data.frame(records = wpdx$`#source`), by=list(wpdx$`#source`), length)
#reporting-org
#participating-org (Implementing)
#participating-org (Accountable)
#participating-org (Extending)
organisationsIATI1 <- aggregate(data.frame(records = iati$`reporting-org`), by=list(iati$`reporting-org`), length)
organisationsIATI2 <- aggregate(data.frame(records = iati$`participating-org (Implementing)`), by=list(iati$`participating-org (Implementing)`), length)
organisationsIATI3 <- aggregate(data.frame(records = iati$`participating-org (Accountable)`), by=list(iati$`participating-org (Accountable)`), length)
organisationsIATI4 <- aggregate(data.frame(records = iati$`participating-org (Extending)`), by=list(iati$`participating-org (Extending)`), length)

orgs <- rbind(organisationsIATI1,organisationsIATI2, organisationsIATI3, organisationsIATI4, organisationsWPDX)

orgs <- aggregate(orgs, by=list(orgs$`Group.1`), length)

orgs <- addNodeId(orgs, n_nodes)
n_nodes <- max(orgs$node_id)

#SKIPPING installer BECAUSE OF JUNK DATA --> NEED TO COMBINE AUTOMATICALLY USING SIMILARITY...
#installer <- aggregate(data.frame(records = wpdx$`#installer`), by=list(wpdx$`#installer`), length)
#c(organisations$Group.1,installer$Group.1)


#cleaning up the links variables for the datasets
wpdx$data_lnk = ""
wpdx[!is.na(wpdx$`#data_lnk`),]$data_lnk <- wpdx[!is.na(wpdx$`#data_lnk`),]$`#data_lnk`
wpdx$orig_lnk = ""
wpdx[!is.na(wpdx$`#orig_lnk`),]$orig_lnk <- wpdx[!is.na(wpdx$`#orig_lnk`),]$`#orig_lnk`


wpdx_datasets <- aggregate(data.frame(records = wpdx[,c("data_lnk","orig_lnk")]), 
                      by=list(wpdx$data_lnk, wpdx$orig_lnk), FUN = function(x,y) {1})

wpdx_datasets <- addNodeId(wpdx_datasets, n_nodes)
n_nodes <- max(wpdx_datasets$node_id)

iati_projects <- aggregate(data.frame(records = iati$title), 
                           by=list(iati$title), length)
iati_projects <- addNodeId(iati_projects, n_nodes)
n_nodes <- max(iati_projects$node_id)

#merging in order to create edges

wpdx_m <- mergeDS(x = wpdx, y=countries, groupVar = 'code', oldNode = 'node_id', by.x = '#country_id', 
                  by.y = 'code', newNode = 'cntry_id')
wpdx_m <- mergeDS(x = wpdx_m, y=orgs, groupVar = 'Group.1', oldNode = 'node_id', by.x = '#source', 
                  by.y = 'Group.1', newNode = 'org_id')

wpdx_m <- merge(x=wpdx_m, y=datasets[,c('Group.1','Group.2','node_id')], by.x = c('data_lnk','orig_lnk'), 
                by.y = c('Group.1','Group.2'), all.x = TRUE)
wpdx_m$dataset_id <- wpdx_m$node_id
wpdx_m$node_id <- NULL

iati_m <- mergeDS(x = iati, 
                  y=orgs, 
                  groupVar = 'Group.1', 
                  oldNode = 'node_id', 
                  by.x = 'reporting-org',
                  by.y = 'Group.1',
                  newNode = 'org1_id')
iati_m <- mergeDS(x = iati_m, 
                  y=orgs, 
                  groupVar = 'Group.1', 
                  oldNode = 'node_id', 
                  by.x = 'participating-org (Implementing)',
                  by.y = 'Group.1',
                  newNode = 'org2_id')
iati_m <- mergeDS(x = iati_m, 
                  y=orgs, 
                  groupVar = 'Group.1', 
                  oldNode = 'node_id', 
                  by.x = 'participating-org (Accountable)',
                  by.y = 'Group.1',
                  newNode = 'org3_id')
iati_m <- mergeDS(x = iati_m, 
                  y=orgs, 
                  groupVar = 'Group.1', 
                  oldNode = 'node_id', 
                  by.x = 'participating-org (Extending)',
                  by.y = 'Group.1',
                  newNode = 'org4_id')
iati_m <- mergeDS(x = iati_m, 
                  y=orgs, 
                  groupVar = 'Group.1', 
                  oldNode = 'node_id', 
                  by.x = 'participating-org (Extending)',
                  by.y = 'Group.1',
                  newNode = 'org4_id')
iati_m <- mergeDS(x = iati_m, 
                  y=countries, 
                  groupVar = 'code', 
                  oldNode = 'node_id', 
                  by.x = 'recipient-country-code',
                  by.y = 'code',
                  newNode = 'cntry_id')
iati_m <- mergeDS(x = iati_m, 
                  y=iati_projects, 
                  groupVar = 'Group.1', 
                  oldNode = 'node_id', 
                  by.x = 'title',
                  by.y = 'Group.1',
                  newNode = 'project_id')



iati_project_country <- aggregate(
  data.frame(
    records = iati_m[,
                     c("project_id",
                       "cntry_id"
                     )]), 
  by=list(
    iati_m$project_id, 
    iati_m$cntry_id), 
  length)


#########################################org1
iati_org1_country <- aggregate(
                        data.frame(
                          records = iati_m[,
                            c("org1_id",
                              "cntry_id"
                            )]), 
                        by=list(
                          iati_m$org1_id, 
                          iati_m$cntry_id), 
                        length)

iati_org1_project <- aggregate(
  data.frame(
    records = iati_m[,
                     c("org1_id",
                       "project_id"
                     )]), 
  by=list(
    iati_m$org1_id, 
    iati_m$project_id), 
  length)


#########################################org2
iati_org2_country <- aggregate(
  data.frame(
    records = iati_m[,
                     c("org2_id",
                       "cntry_id"
                     )]), 
  by=list(
    iati_m$org2_id, 
    iati_m$cntry_id), 
  length)

iati_org2_project <- aggregate(
  data.frame(
    records = iati_m[,
                     c("org2_id",
                       "project_id"
                     )]), 
  by=list(
    iati_m$org2_id, 
    iati_m$project_id), 
  length)


#########################################org3
iati_org3_country <- aggregate(
  data.frame(
    records = iati_m[,
                     c("org3_id",
                       "cntry_id"
                     )]), 
  by=list(
    iati_m$org3_id, 
    iati_m$cntry_id), 
  length)

iati_org3_project <- aggregate(
  data.frame(
    records = iati_m[,
                     c("org3_id",
                       "project_id"
                     )]), 
  by=list(
    iati_m$org3_id, 
    iati_m$project_id), 
  length)


#########################################org4
iati_org4_country <- aggregate(
  data.frame(
    records = iati_m[,
                     c("org4_id",
                       "cntry_id"
                     )]), 
  by=list(
    iati_m$org4_id, 
    iati_m$cntry_id), 
  length)

iati_org4_project <- aggregate(
  data.frame(
    records = iati_m[,
                     c("org4_id",
                       "project_id"
                     )]), 
  by=list(
    iati_m$org4_id, 
    iati_m$project_id), 
  length)

##


wpdx_country_organisation <- aggregate(data.frame(records = wpdx_m[,c("cntry_id","org_id")]), by=list(wpdx_m$cntry_id, wpdx_m$org_id), length)
wpdx_country_dataset <- aggregate(data.frame(records = wpdx_m[,c("cntry_id","dataset_id")]), by=list(wpdx_m$cntry_id, wpdx_m$dataset_id), length)
wpdx_dataset_organisation <- aggregate(data.frame(records = wpdx_m[,c("dataset_id","org_id")]), by=list(wpdx_m$dataset_id, wpdx_m$org_id), length)

edges <- data.frame(`source` = 
                      c(wpdx_country_organisation$Group.1, 
                        wpdx_country_dataset$Group.1, 
                        wpdx_dataset_organisation$Group.1,
                        iati_org1_country$Group.1,
                        iati_org1_project$Group.1,
                        iati_project_country$Group.1,
                        iati_org2_country$Group.1,
                        iati_org2_project$Group.1,
                        iati_org3_country$Group.1,
                        iati_org3_project$Group.1,
                        iati_org4_country$Group.1,
                        iati_org4_project$Group.1
                        ), 
                    target = 
                      c(wpdx_country_organisation$Group.2, 
                        wpdx_country_dataset$Group.2, 
                        wpdx_dataset_organisation$Group.2,
                        iati_org1_country$Group.2,
                        iati_org1_project$Group.2,
                        iati_project_country$Group.2,
                        iati_org2_country$Group.2,
                        iati_org2_project$Group.2,
                        iati_org3_country$Group.2,
                        iati_org3_project$Group.2,
                        iati_org4_country$Group.2,
                        iati_org4_project$Group.2
                        )
                    )

nodes <- data.frame(
  ID = c(
    countries$node_id, 
    orgs$node_id, 
    datasets$node_id,
    iati_m$project_id), 
  Label = c(
    countries$name,
    orgs$Group.1,
    paste("Dataset ", datasets$node_id, datasets$Group.1, datasets$Group.2," - "),
    iati_m$title ), 
  Type=c(
    replicate(length(countries$node_id),"Country"),
    replicate(length(orgs$node_id),"Organisation"),
    replicate(length(datasets$node_id),"Dataset"),
    replicate(length(iati_m$project_id),"Project")
    )
  )

write.csv(edges,file = "edges.csv", row.names = FALSE)
write.csv(nodes,file = "nodes.csv", row.names = FALSE)

