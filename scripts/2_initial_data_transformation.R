# Set the working directory if not working in working within the project
# setwd("path/to/ds_code_challenge")

# a vector containing the names of all the packages used in the document
req_pkg <- c(
  "paws",
  "httr2",
  "tictoc",
  "logger",
  "glue",
  "sf",
  "jsonlite",
  "tmap",
  "furrr",
  "parallel",
  "janitor",
  "glue",
  "h3jsr",
  "osrm",
  "readODS",
  "digest",
  "lwgeom",
  "tidyverse"
)
# vector indicating whether the packages in req_pkg are installed
filter_vec <- !req_pkg %in% installed.packages()[,"Package"]
# install those that have not been installed
if(sum(filter_vec) > 0) walk(pkg[filter_vec], ~install.packages(.x, Ncpus = 5))

library(httr2)
library(tmap)
library(sf)
library(lwgeom)
library(h3jsr)
library(logger)
library(tictoc)
library(glue)
library(furrr)
library(parallel)
library(tidyverse)
source("scripts/helpers/download_cpt_s3_data.R")
source("scripts/helpers/st_join_contains.R")

# retrieve the file if it doesn't exist yet -------------------------------

# execute the download_cpt_s3_data function to retrieve the file if it doesn't 
# exist & provide the path if it does exist
tic("Time taken to retrieve sr_hex")
download_cpt_s3_data("https://cct-ds-code-challenge-input-data.s3.af-south-1.amazonaws.com/sr_hex.csv.gz")
download_cpt_s3_data("https://cct-ds-code-challenge-input-data.s3.af-south-1.amazonaws.com/sr.csv.gz")
toc()


# load the data and prepare for spatial join ------------------------------

# read in the service request data with the H3 hex indexes for the past 12 
# months
service_requests_12m <- read_csv("data/sr.csv") %>% 
  # exclude the first column, it is an unnamed row counter
  .[,-1]

# keep the rows with missing lat & long values separate from the geo join data
missing_geo_sr_12m <- service_requests_12m %>% 
  filter(is.na(latitude) & is.na(longitude)) %>% 
  # we only need to keep the notification_number as the unique ID
  select(notification_number) %>% 
  # set the index to zero as instructed
  mutate(h3_level8_index = "0")

# create an object with only non-missing coordinates 
geo_sr_12m <- service_requests_12m %>% 
  # select only the notification number as the unique ID
  select(notification_number, latitude, longitude) %>% 
  # filter out rows with missing geolocation
  filter(!is.na(latitude) & !is.na(longitude))


# joining the SR data to the polygons -------------------------------------
# ensure the file to validate against is downloaded
tic("Time taken to download file (or check if already downloaded)")
hex_polygons_8_location <- "https://cct-ds-code-challenge-input-data.s3.af-south-1.amazonaws.com/city-hex-polygons-8.geojson" 
download_cpt_s3_data(hex_polygons_8_location)
toc()
# read in the polygons
city_hex_8 <- st_read("data/city-hex-polygons-8.geojson")

tic("Time consumed in conversion and spatial join")
# convert the lat & long points into point geometries
geom_point_values <- map2(
  geo_sr_12m$longitude,
  geo_sr_12m$latitude,
  ~st_point(c(.x, .y))
)

sf_sr_12m <- geo_sr_12m %>% 
  # it is important to check the CRS of the hex to ensure they match
  st_sf(geometry = geom_point_values, crs = 4326)

# iterating over 10 smaller dataframes is more memory efficient than performing 
# the join over the entire points dataframe. On my machine it is also faster,
# it is also faster than implementing it in parallel (but only because I am 
# limited to 8GB of memory). I implemented a parallel version over 5 instances
# but the excessive memory usage made it perform slower than the sequential.
# cl <- makePSOCKcluster(5)  # for parallel
# plan(cluster, workers = cl)  # for parallel
geo_sr_12m_indexed <- sf_sr_12m %>% 
  # create an index to loop over
  mutate(
    row_num = 1:n(),
    row_num = row_num %% 10
  ) %>% 
  # split the dataframe into segments according to the index
  group_nest(row_num, .key = "points_df") %>% 
  # Add the polygons as a column in the dataframe (assists with using pmap). 
  # This will not be memory intensive, all elements of the list will merely
  # be pointers to the same dataframe containing the polygons
  mutate(
    polygons_df = list(city_hex_8)
  ) %>% 
  # now that the data segments have been create, drop the index
  select(-row_num) %>% 
  # iterate over the segments and bind the rows of the resulting sf dataframes.
  # Note that the st_join_contains function is defined in a separate file & 
  # sourced at the start of the script (scripts/st_join_contains.R)
  # future_pmap_dfr(st_join_contains)  # for parallel
  pmap_dfr(st_join_contains)

# stopCluster(cl)  # for parallel

# display the time taken for the operation
toc()

# There are 3 data points that don't comply with the geo join, what should the threshold be? 
failed_records <- geo_sr_12m %>% 
  filter(!notification_number %in% geo_sr_12m_indexed$notification_number)

# save a file to check which records couldn't match
saveRDS(failed_records, "output/failed_geo_join_records.rds")

# If more than 0.1% of rows failed to join, throw an error and explain where the 
# failed records can be found.
# The 0.1% threshold was chosen to ensure a low threshold for detecting 
# systematic issues (not mere anomalies)
if(nrow(failed_records)/nrow(geo_sr_12m) > 0.001){
  log_error(
    glue(
      "THERE WERE {nrow(failed_records)} RECORDS THAT COULD NOT BE JOINED BASED ON COORDINATES
      ____THE 20 RECORD THRESHOLD HAS BEEN VIOLATED____
      FIND THE RECORDS THAT FAILED IN output/failed_geo_join_records.rds"
    )
  )
} else {
  # otherwise, let the user know how many records were not joined
  log_info(
    glue(
      "THERE WERE {nrow(failed_records)} RECORDS THAT COULD NOT BE JOINED BASED ON COORDINATES."
    )
  )
}

# use the sf dataframe for the 12 month service reports to convert the lat long
# coordinates from the rows that failed to join to H3 res 8 index values
outside_cpt_h3 <- sf_sr_12m %>%
  # keep only the rows which were unable to join to the provided polygons
  filter(notification_number %in% failed_records$notification_number) %>%
  # use the point_to_cell function to obtain the h3 res 8 index value for each
  # point
  mutate(
    h3_level8_index = map_chr(geometry, ~point_to_cell(.x, res = 8))
  ) %>% 
  # keep only the notification_number and h3 index variables
  st_drop_geometry() %>% 
  select(
    notification_number, h3_level8_index
  )

# combine records without coordinates, those which were matched, and those which
# were derived
complete_indexed <- bind_rows(
  geo_sr_12m_indexed, 
  missing_geo_sr_12m,
  outside_cpt_h3
) 

all_indexed <- service_requests_12m %>% 
  left_join(complete_indexed)
# display a snippet of the data
all_indexed

# check against original file to see if they match
measure_file <- read_csv("data/sr_hex.csv")
# display a snippet of the data
measure_file

# The zero rows in the dataframe resulting from the anti_join by all variables 
# indicates a perfect match
all_indexed %>% 
  anti_join(measure_file)



