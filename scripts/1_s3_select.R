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

library(paws)
library(httr2)
library(tictoc)
library(logger)
library(glue)
library(sf)
library(jsonlite)
library(tidyverse)
source("scripts/download_cpt_s3_data.R")

# set up AWS access -------------------------------------------------------


# set up the s3 connection (the credentials should be saved in  .Renviron)
s3 <- paws::s3(
  config = list(
    credentials = list(
      creds = list(
        access_key_id = Sys.getenv("AWS_ACCESS_KEY_ID"),
        secret_access_key = Sys.getenv("AWS_SECRET_ACCESS_KEY")
      )
    ),
    region = Sys.getenv("AWS_REGION")
  )
)


# Retrieve the polygons at resolution 8 -----------------------------------


# specify the file information for extraction 
bucket_name <- "cct-ds-code-challenge-input-data"
file_name <- "city-hex-polygons-8-10.geojson"
query <- "SELECT s.* FROM s3object[*].features[*] s WHERE s.properties.resolution = 8"

# start timer for data retrieval operation
tic("Retrieving the data took")
# use the select object content operation to retrieve the data from the file
hex_8_stream <- s3$select_object_content(
  Bucket = bucket_name,
  Key = file_name,
  Expression = query,
  ExpressionType = "SQL",
  InputSerialization = list(
    # geojsons are jsons so we can use the json format
    JSON = list(Type = "DOCUMENT"),
    CompressionType = "NONE"
  ),
  OutputSerialization = list(
    JSON = list(RecordDelimiter = "\n")
  )
)

# AWS S3 Select streams the response (because the size of the request is 
# unknown). The code below retrieves all the chunks from the response
hex_8_events <- hex_8_stream$Payload(
  function(x) {
    
    if (!is.null(x$Records)){
      output <- x$Records$Payload
      
      return(x)
      
    }
  }
)

# Define a function that can be used to loop over the events to extract text 
# from the raw response
extract_from_payload <- function(raw){
  
  if(is.raw(raw$Records$Payload)){
    return_char <- rawToChar(raw$Records$Payload)
  } else{
    return_char <- ""
  }
  
  return(return_char)
  
}

# extract all of the valid raw data that was streamed
map_chr(
  hex_8_events,
  extract_from_payload
) %>% 
  # collapse into a single string
  str_c(collapse = "") %>% 
  # remove trailing white space
  str_trim() %>% 
  # save the cleaned data in the appropriate file location
  writeLines(text = ., con = "data/city_hex_polygons_8_10_filter_8.geojson")

# end timer for the operation
toc()

# Read in the geojsons for validation -------------------------------------

# read selected data in
cpt_hex_filtered_8 <- st_read("data/city_hex_polygons_8_10_filter_8.geojson")

# ensure the file to validate against is downloaded
tic("Time taken to download file (or check if already downloaded)")
hex_polygons_8_location <- "https://cct-ds-code-challenge-input-data.s3.af-south-1.amazonaws.com/city-hex-polygons-8.geojson" 
download_cpt_s3_data(hex_polygons_8_location)
toc()

# read the file in against which to validate
city_polygons_8 <- st_read("data/city-hex-polygons-8.geojson")

# validate that the two files are similar ---------------------------------

# start the timer to log the time taken for validation
tic("Validation of city-hex-polygons-8-10 (res 8) against city-hex-polygons-8")

# get the number of rows in the larger file that are not contained in the smaller
# file
rows_8to10_not_in_8 <- cpt_hex_filtered_8 %>% 
  filter(resolution == 8) %>% 
  filter(!index %in% city_polygons_8$index) %>% 
  nrow()

# ge the number of rows in the smaller file that are not contained in the larger
# file
rows_8_not_in_8to10 <- city_polygons_8 %>% 
  filter(!index %in% cpt_hex_filtered_8$index) %>% 
  nrow()

if(rows_8_not_in_8to10 == 0 & rows_8to10_not_in_8 == 0){
  log_info(
    "Exact match between city-hex-polygons-8 & city-hex-polygons-8-10 (res 8)"
  )
} else{
  log_info(
    glue("city-hex-polygons-8-10 (res 8) has {rows_8to10_not_in_8} rows that don't match city-hex-polygons-8")
  )
  log_info(
    glue("city-hex-polygons-8 has {rows_8_not_in_8to10} rows that don't match city-hex-polygons-8-10 (res 8)")
  )
}

# report the time consumed by the operation
toc()
