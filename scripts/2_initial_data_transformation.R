library(httr2)
library(tmap)
library(sf)
library(logger)
library(tictoc)
library(glue)
library(tidyverse)
source("scripts/download_cpt_s3_data.R")

# retrieve the file if it doesn't exist yet -------------------------------

# execute the download_cpt_s3_data function to retrieve the file if it doesn't 
# exist & provide the path if it does exist
tic("Time taken to retrieve sr_hex")
download_cpt_s3_data("https://cct-ds-code-challenge-input-data.s3.af-south-1.amazonaws.com/sr_hex.csv.gz")
download_cpt_s3_data("https://cct-ds-code-challenge-input-data.s3.af-south-1.amazonaws.com/sr.csv.gz")
toc()


# load the data and prepare for spatial join ------------------------------
# result <- read_csv("data/sr_hex.csv") 

# read in the service request data with the H3 hex
service_requests_12m <- read_csv("data/sr.csv") %>% 
  # exclude the first column, it is a mere row counter
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
  filter(!is.na(latitude) & !is.na(longitude))


# joining the SR data to the polygons -------------------------------------
# read in the polygons
city_hex_8 <- st_read("data/city-hex-polygons-8.geojson")

# ________________________OPTIMISE & PARALLELISE________________________
tic("Time consumed in conversion and spatial join")
# convert the lat & long points into a geometry to make a sf object of the data
geom_point_values <- map2(
  geo_sr_12m$longitude,
  geo_sr_12m$latitude,
  ~st_point(c(.x, .y))
)

sf_sr_12m <- geo_sr_12m %>% 
  # select only the notification number as the unique ID
  select(notification_number) %>% 
  # it is important to check the CRS of the hex to ensure they match
  st_sf(geometry = geom_point_values, crs = 4326)

# read in the hex data for the city

# this join results in 731,019 rows, the sr data has 941,634 rows
hex_point_sr_join <- st_join(
  x = city_hex_8,
  y = sf_sr_12m,
  join = st_contains
)
toc()

# keep only the necessary data
geo_sr_12m_indexed <- hex_point_sr_join %>%
  st_drop_geometry() %>% 
  as_tibble() %>% 
  select(
    h3_level8_index = index,
    notification_number
  ) %>% 
  filter(!is.na(notification_number))

# There are 3 data points that don't comply with the geo join, what should the threshold be? 
failed_records <- geo_sr_12m %>% 
  filter(!notification_number %in% geo_sr_12m_indexed$notification_number)

# save a file to check which records couldn't match
saveRDS(failed_records, "output/failed_geo_join_records.rds")

# log an error if the 20 record threshold is violated
if(length(failed_records) > 20){
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

# combine records without coordinates and those with matched
complete_indexed <- bind_rows(geo_sr_12m_indexed, missing_geo_sr_12m)

all_indexed <- service_requests_12m %>% 
  left_join(complete_indexed)
# display a snippet of the data
all_indexed

# check against original file to see if they match
measure_file <- read_csv("data/sr_hex.csv")
# display a snippet of the data
measure_file

# only the three records that could not be joined, do not match
all_indexed %>% 
  anti_join(measure_file)

