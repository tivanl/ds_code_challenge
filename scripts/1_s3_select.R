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

hex_8_stream <- s3$select_object_content(
  Bucket = bucket_name,
  Key = file_name,
  Expression = query,
  ExpressionType = "SQL",
  InputSerialization = list(
    JSON = list(Type = "DOCUMENT"),
    CompressionType = "NONE"
  ),
  OutputSerialization = list(
    JSON = list(RecordDelimiter = "\n")
  )
)

hex_8_events <- hex_8_stream$Payload(
  function(x) {
    if (!is.null(x$Records)){
      output <- x$Records$Payload
      
      return(x)
      
    }
  }
)

# extract all of the valid raw data that was streamed
map_chr(
  hex_8_events,
  ~{
    if(is.raw(.x$Records$Payload)){
      rawToChar(.x$Records$Payload)
    } else{
      ""
    }
  }
) %>% 
  str_c(collapse = "") %>% 
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
