# uncomment to install packages
# install.packages('DBI')
# install.packages('RSQLite')
# load DBI and RSQLite packages
library(DBI)
library(RSQLite)
# establish a connection to database
con=dbConnect(SQLite(), dbname="/g/data1/unofficial-ESG-replica/tmp/tree/cmip5_raijin_latest.db")
# list all tables from database
alltables = dbListTables(con)
print(alltables)
# list fields for selected table
dbListFields(con, "instances")
dbListFields(con, "versions")
# execute SQL query
# get all fields from table instances where model/exp/mip/variable match constraints
res <- dbSendQuery(con, "SELECT * FROM instances WHERE model='MIROC5' and experiment='rcp85' and mip='Amon' and variable='tas'")
# returns a sql query object not the query results
print(res)
# fetch query results
dbFetch(res)
# execute query on versions table passing directly instances_id returned from previous query
# NB I don't use enough R to know how to make this more efficient you should be bale to pass the idnexes as an array 
res2 <- dbSendQuery(con,"SELECT * FROM versions WHERE instance_id in (9006, 28476, 39134, 45410, 79734)")
# fecth reuslts
dbFetch(res2)
# same but returns on selected fields version, path and if is_latest?
res2 <- dbSendQuery(con,"SELECT version, path, is_latest FROM versions WHERE instance_id in (9006, 28476, 39134, 45410, 79734)")
# NB there must be a better way to display results by row rather than column again this is limted by my not knowing R
dbFetch(res2)
# use SQL inner join to connect tables at query level
# select ensemble from instances table and version and path from versions table
res <- dbSendQuery(con, "SELECT ensemble, version, path FROM instances INNER JOIN versions USING(instance_id) WHERE model='MIROC5' and experiment='rcp85' and mip='Amon' and variable='tas'")
dbFetch(res)
