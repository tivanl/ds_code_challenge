library(httr2)
library(janitor)
library(glue)
library(osrm)
library(tmap)
library(logger)
library(sf)
library(readODS)
library(tidyverse)
source("scripts/load_cpt_suburbs.R")

# load the service request data (indexed) and the polygons vector data
sr_hex_df <- read_csv("data/sr_hex.csv") %>% 
  filter(!is.na(latitude) & !is.na(longitude))
cpt_polygons_8 <- st_read("data/city-hex-polygons-8.geojson")

# Visualise the number of service requests for Bellville South per polygon
belville_south_sr <- sr_hex_df %>% 
  filter(official_suburb == "BELLVILLE SOUTH") %>% 
  rename(index = h3_level8_index) %>% 
  count(index, name = "sr_belville_south") %>% 
  left_join(
    cpt_polygons_8,
    .
  ) 

belville_south_sr %>% 
  tm_shape() +
  tm_polygons() +
  tm_fill("sr_belville_south", breaks = c(0, 10, 50, 100, 200, 400, 600, 800, 1000, 1200, 1500))

# download the municipal area shape file for cpt 
tic("Time taken to load the suburb shape file")
cpt_offcial_suburb_shp <- load_cpt_suburbs()
toc()

tic("Time taken to derive the centroid of Bellville South")
belville_centroid <- cpt_offcial_suburb_shp %>% 
  filter(OFC_SBRB_N == "BELLVILLE SOUTH") %>% 
  .$geometry %>% 
  st_centroid() 
toc()

# Determine the area which is reachable within 1 min or less from the centroid
tic("Time taken to determine the 1 min reachable area from the centroid")
centroid_buff_1min <- belville_centroid %>% 
  # transform to the appropriate coordinate reference system
  st_transform(4326) %>% 
  # extract a vector containing the long and lat values for the centroid
  st_coordinates() %>% 
  # use the OSM API to retrieve the area within 1 min from the centroid
  osrmIsochrone(breaks = 1, res = 30) %>% 
  st_make_valid()
toc()


# visualise the process using the Bellville South shape
cpt_offcial_suburb_shp %>% 
  filter(OFC_SBRB_N == "BELLVILLE SOUTH") %>% 
  tm_shape() +
  tm_borders() +
  tm_shape(centroid_buff_1min) +
  tm_polygons(fill_alpha = 0.7, fill = "blue") + 
  tm_shape(belville_centroid) +
  tm_dots(fill = "red") +
  tm_layout(frame = FALSE) +
  tm_title("Bellville South outline, centroid (red), and area within 1min travel (blue)")

# Use the long & lat values from sr & filter out all points outside the 1m area

geom_point_values <- map2(
  sr_hex_df$longitude,
  sr_hex_df$latitude,
  ~st_point(c(.x, .y))
)

sr_geom_df <- sr_hex_df %>% 
  # it is important to check the CRS of the hex to ensure they match
  st_sf(geometry = geom_point_values, crs = 4326) 

# get the service reports with location in the 1 min area from the centroid
sr_bellville_centroid_1m <- sr_geom_df %>% 
  st_join(centroid_buff_1min, join = st_within) %>% 
  filter(!is.na(id)) %>% 
  select(-c(id, isomin, isomax))

adjacent_burbs <- sr_bellville_centroid_1m %>% 
  distinct(official_suburb) %>% 
  pull(official_suburb)

# Display the process
cpt_offcial_suburb_shp %>% 
  filter(OFC_SBRB_N == "BELLVILLE SOUTH") %>% 
  tm_shape() +
  tm_borders() +
  tm_shape(sr_geom_df %>% filter(official_suburb %in% adjacent_burbs)) +
  tm_dots(fill = "green", fill_alpha = 0.3) +
  tm_shape(centroid_buff_1min) +
  tm_polygons(fill_alpha = 0.7, fill = "blue") + 
  tm_layout(frame = FALSE) +
  tm_title("Bellville South outline (black)\nArea within 1min travel (blue)\nLocation of service reports (green)")

# Display the process
cpt_offcial_suburb_shp %>% 
  filter(OFC_SBRB_N == "BELLVILLE SOUTH") %>% 
  tm_shape() +
  tm_borders() +
  tm_shape(centroid_buff_1min) +
  tm_polygons(fill_alpha = 0.7, fill = "blue") + 
  tm_shape(sr_bellville_centroid_1m) +
  tm_dots(fill = "yellow", fill_alpha = 0.3) +
  tm_layout(frame = FALSE) +
  tm_title("Bellville South outline (black)\nArea within 1min travel (blue)\nLocation of service reports in area (yellow)")


# 2. Bring in the wind data
wind_req_link <- "https://www.capetown.gov.za/_layouts/OpenDataPortalHandler/DownloadHandler.ashx?DocumentName=Wind_direction_and_speed_2020.ods&DatasetDocument=https%3A%2F%2Fcityapps.capetown.gov.za%2Fsites%2Fopendatacatalog%2FDocuments%2FWind%2FWind_direction_and_speed_2020.ods"

request(wind_req_link) %>% 
  req_perform(path = "data/wind_direction_and_speed_2020.ods")

bellville_wind_df <-
  read_ods("data/wind_direction_and_speed_2020.ods", skip = 2) %>% 
  # clean the names of the columns
  clean_names() %>% 
  select(
    date_time,
    # select only the columns for belville
    contains("bellville")
  ) %>% 
  # give the columns appropriate names
  set_names(c("date_time", "wind_dir_deg", "wind_speed_m_sec")) %>% 
  mutate(
    date_time = dmy_hms(glue("{date_time}:00")),
    wind_dir_deg = as.double(wind_dir_deg),
    wind_speed_m_sec = as.double(wind_speed_m_sec)
  ) %>% 
  # The rows where date_time is missing are not applicable (they contained 
  # unnecessary or no information)
  filter(!is.na(date_time)) 

# now we need the air quality to join to filtered points
sr_bellville_wind <- sr_bellville_centroid_1m %>%
  # the weather data is captured at an hourly frequency, create an hourly column
  # that can be used to perform the join
  mutate(
    creation_hour = floor_date(creation_timestamp, unit = "hours"),
    .before = 3
  ) %>% 
  left_join(bellville_wind_df, by = join_by("creation_hour" == "date_time")) 

# 3. anonymise the data
sr_bellville_wind %>% glimpse()
sr_bellville_wind %>% 
  select(
   notification_number,
   reference_number,
   creation_timestamp,
   completion_timestamp,
   directorate,
   department,
   branch,
   section,
   code_group,
   code,
   cause_code,
   official_suburb,
   h3_level8_index,
   wind_dir_deg,
   wind_speed_m_sec
  ) %>% 
  mutate(
    creation_timestamp_6h = floor_date(creation_timestamp, "6H")
  ) %>% View()
