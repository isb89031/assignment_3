# set the working directory that contains the files
setwd("C:/Users/sangbin.lim/Desktop/¿”ªÛ∫Û/«–±≥/ST2195/P03_Practice_Suggested_Solution-20211110T120217Z-001/P03_Practice_Suggested_Solution/r_sql")
getwd()

# ===== load libraries ======

#install.packages("RSQLite")
#install.packages("dbplyr")
library(DBI)
library(dplyr)

# ===== create the databases =====

if (file.exists("airline_v2.db"))
  file.remove("airline_v2.db")

conn <- dbConnect(RSQLite::SQLite(), "airline_v2.db")


# ===== write to the database =====

# load in the data from the csv files
airports <- read.csv("./dataverse_files/airports.csv", header = TRUE)
carriers <- read.csv("./dataverse_files/carriers.csv", header = TRUE)
planes <- read.csv("./dataverse_files/plane-data.csv", header = TRUE)
dbWriteTable(conn, "airports", airports)
dbWriteTable(conn, "carriers", carriers)
dbWriteTable(conn, "planes", planes)

# create a loop to load 2000-2005 ontime data files directly from compressed bz2 format
# this will take some time
# you could also unzip bz2 format individually into .csv files

for(i in c(2000:2005)) {
  filename <- paste0("./dataverse_files/", i, ".csv.bz2")
  print(paste("Processing:", i))
  ontime <- read.csv(filename, header = TRUE)
  if(i == 2000) {
    dbWriteTable(conn, "ontime", ontime)
  } else {
    dbWriteTable(conn, "ontime", ontime, append = TRUE)
  }
}

# alternatively, load multiple csv individually into data frames
# then use rbind to append

#ontime_2000 <- read.csv(".dataverse_files/2000.csv", header = TRUE)
#ontime_2001 <- read.csv(".dataverse_files/2001.csv", header = TRUE)
#ontime_2002 <- read.csv(".dataverse_files/2002.csv", header = TRUE)
#ontime_2003 <- read.csv(".dataverse_files/2003.csv", header = TRUE)
#ontime_2004 <- read.csv(".dataverse_files/2004.csv", header = TRUE)
#ontime_2005 <- read.csv(".dataverse_files/2005.csv", header = TRUE)

#ontime_all <- ontime_2000 %>%
#  rbind(ontime_2001) %>%
#  rbind(ontime_2002) %>%
#  rbind(ontime_2003) %>%
#  rbind(ontime_2004) %>%
#  rbind(ontime_2005)

#dbWriteTable(conn, "ontime", ontime_all)

# remove temporary dataframe
#rm(ontime_2000,ontime_2001,ontime_2002,ontime_2003,ontime_2004,ontime_2005)

# garbage collection
gc()


dbListTables(conn)
dbListFields(conn, "ontime")


# ===== warm up queries =====

# number of flights from years 2000 to 2005
dbGetQuery(conn,
"SELECT Year, COUNT(*)
FROM ontime
GROUP BY Year"
)

# summary of flight departure delays
dbGetQuery(conn,
"SELECT
     COUNT(DISTINCT TailNum) AS distinct_aircraft,
     COUNT(FlightNum) AS flights,
     AVG(DepDelay) AS avg_departure_delay,
     MAX(DepDelay) AS max_departure_delay,
     MIN(DepDelay) AS min_departure_delay
FROM ontime
WHERE Cancelled=0 AND Diverted=0"
)

# most popular routes from 2000 to 2005
## || = combine all items
dbGetQuery(conn,
"SELECT
     COUNT(FlightNum) AS flights,
     (Origin || '-' || Dest) as Route
FROM ontime
GROUP by Route
ORDER BY flights DESC
LIMIT 10"
)

# top airlines by flights across 2000 to 2005
dbGetQuery(conn,
"SELECT
     carriers.Description as airline_name,
     ontime.UniqueCarrier as airline_code,
     COUNT(ontime.FlightNum) AS flights
FROM ontime LEFT JOIN carriers ON ontime.UniqueCarrier=carriers.Code
GROUP by airline_code
ORDER BY flights DESC
LIMIT 3"
)

# most popular destination cities
dbGetQuery(conn,
"SELECT
     airports.city,
     COUNT(*) AS flights
FROM ontime INNERJOIN airports 
GROUP by airports.city
ORDER BY flights DESC
LIMIT 10"
)


# ========== Queries via DBI ==========

# Q1 ?Ä? find the plane model with lowest average departure delay

# model field is in plane table
# select flights not cancelled or diverted, and departure delay > 0
# if unspecified, default for JOIN is INNER JOIN

q1 <- dbGetQuery(conn,
"SELECT model AS model, AVG(ontime.DepDelay) AS avg_delay
 FROM planes JOIN ontime USING(tailnum)
 WHERE ontime.Cancelled = 0 AND ontime.Diverted = 0 AND ontime.DepDelay > 0
 GROUP BY model
 ORDER BY avg_delay"
)
print(paste(q1[1, "model"], "has the lowest associated average departure delay"))


# Q2 ?Ä? find the city with highest number of inbound flights

# city field is in airports table
# iata in airports table is the iata airport code
# origin and dest in ontime table contains the iata airport code
# exclude cancelled flights

q2 <- dbGetQuery(conn,
"SELECT airports.city AS city, COUNT(*) AS total
 FROM airports JOIN ontime ON ontime.dest = airports.iata
 WHERE ontime.Cancelled = 0
 GROUP BY airports.city
 ORDER BY total DESC"
)
print(paste(q2[1, "city"], "has the highest number of inbound flights (excluding cancelled flights)"))


# Q3 ?Ä? find the carrier with highest number of cancelled flights

# UniqueCarrier field in ontime table contains the carrier code
# Code field in carriers table contains the carrier code

q3 <- dbGetQuery(conn,
"SELECT carriers.Description AS carrier, COUNT(*) AS total
 FROM carriers JOIN ontime ON ontime.UniqueCarrier = carriers.Code
 WHERE ontime.Cancelled = 1
 GROUP BY carriers.Description
 ORDER BY total DESC"
)
print(paste(q3[1, "carrier"], "has the highest number of cancelled flights"))

# Q4 ?Ä? find the carrier with highest ratio of cancelled flights to total flights

# create 2 sub-queries:
#  (i) number of cancelled flights - numerator
#  (ii) number of flights - denominator
# join the 2 sub-queries
# CAST function is needed to convert INTEGER (in counts) to FLOAT

q4 <- dbGetQuery(conn,
"SELECT q1.carrier AS carrier, (CAST(q1.numerator AS FLOAT) / CAST(q2.denominator AS FLOAT)) AS ratio
FROM
(
  SELECT carriers.Description AS carrier, COUNT(*) AS numerator
  FROM carriers JOIN ontime ON ontime.UniqueCarrier = carriers.Code
  WHERE ontime.Cancelled = 1
  GROUP BY carriers.Description
) AS q1 JOIN
(
  SELECT carriers.Description AS carrier, COUNT(*) AS denominator
  FROM carriers JOIN ontime ON ontime.UniqueCarrier = carriers.Code
  GROUP BY carriers.Description
) AS q2 USING(carrier)
ORDER BY ratio DESC")
print(paste(q4[1, "carrier"], "highest number of cancelled flights, relative to their number of total flights at: ", round(q4[1, "ratio"],5)))

# simplified Q4 soltion
q4_simplified <- dbGetQuery(conn,
"SELECT carriers.Description as carrier, avg(ontime.Cancelled) as cancelled_ratio
FROM ontime JOIN carriers ON ontime.UniqueCarrier = carriers.Code
GROUP BY carrier
ORDER BY cancelled_ratio DESC")
print(paste(q4_simplified[1, "carrier"], "highest number of cancelled flights, relative to their number of total flights at: ", round(q4_simplified[1, "cancelled_ratio"],5)))


# ========== Queries via dplyr ==========

planes_db <- tbl(conn, "planes")
ontime_db <- tbl(conn, "ontime")
carriers_db <- tbl(conn, "carriers")
airports_db <- tbl(conn, "airports")

# Q1 ?Ä? find the plane model with lowest average departure delay

# rename all variable names to lower case to facilitate join "by" tailnum
# "TailNum" used as variable name in planes table
# "tailnum" used as variable name in ontime table

q1 <- ontime_db %>% 
  rename_all(tolower) %>%
  inner_join(planes_db, by = "tailnum", suffix = c(".ontime", ".planes")) %>%
  filter(Cancelled == 0 & Diverted == 0 & DepDelay > 0) #%>%
  #group_by(model) %>%
  #summarize(avg_delay = mean(DepDelay, na.rm = TRUE)) %>%
  #arrange(avg_delay) 
print(head(q1,1))

# Alternative Q1. plane model with lowest average delay

t1 <- ontime_db %>% rename(Flight_Year=Year,tailnum=TailNum)
t2 <- planes_db %>% rename(plane_year=year)
q1b <- t1 %>%
  inner_join(t2, by="tailnum") %>%
  filter(Cancelled==0 & Diverted==0 & DepDelay>0) %>%
  group_by(model) %>%
  summarize(avg_delay = mean(DepDelay, na.rm = TRUE)) %>%
  arrange(avg_delay)
print(head(q1b, 1))


# Q2. city with highest number of inbound flights 

q2 <- ontime_db %>% 
  inner_join(airports_db, by = c("Dest" = "iata")) %>%
  filter(Cancelled == 0) %>%
  group_by(city) %>%
  summarize(total = n()) %>%
  arrange(desc(total)) 
print(head(q2, 1))


# Q3.  carrier with highest number of cancelled flights

q3 <- ontime_db %>% 
  inner_join(carriers_db, by = c("UniqueCarrier" = "Code")) %>%
  filter(Cancelled == 1) %>%
  group_by(Description) %>%
  summarize(total = n()) %>%
  arrange(desc(total))
print(head(q3, 1))


# Q4 carrier with highest ratio of cancelled flights to total flights  

q4a <- inner_join(ontime_db, carriers_db, by = c("UniqueCarrier" = "Code")) %>%
  filter(Cancelled == 1) %>%
  group_by(Description) %>%
  summarize(numerator = n()) %>%
  rename(carrier = Description)

q4b <- inner_join(ontime_db, carriers_db, by = c("UniqueCarrier" = "Code")) %>%
  group_by(Description) %>%
  summarize(denominator = n()) %>%
  rename(carrier = Description)

q4 <- inner_join(q4a, q4b, by = "carrier") %>%
  #mutate_if(is.integer, as.double) %>%
  mutate(numerator = as.double(numerator)) %>%
  mutate(denominator = as.double(denominator)) %>%
  mutate(ratio = numerator/denominator) %>%
  select(carrier, ratio) %>%
  arrange(desc(ratio)) 

print(head(q4, 1))


# Simplifed Q4 solution

q4_simplified <- inner_join(ontime_db, carriers_db, by = c("UniqueCarrier" = "Code")) %>%
  rename(carrier = Description) %>%
  group_by(carrier) %>%
  summarise(ratio = mean(Cancelled, na.rm = TRUE)) %>%
  arrange(desc(ratio))
print(head(q4_simplified, 1))


# ======== disconnect database ========

dbDisconnect(conn)



