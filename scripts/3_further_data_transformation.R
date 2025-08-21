library(httr2)
library(digest)
library(lwgeom)
library(furrr)
library(parallel)
library(tictoc)
library(janitor)
library(glue)
library(osrm)
library(tmap)
library(logger)
library(sf)
library(readODS)
library(tidyverse)
source("scripts/helpers/load_cpt_suburbs.R")
source("scripts/helpers/download_file.R")

options(scipen = 999)

# load the service request data (indexed) and the polygons vector data
sr_hex_df <- read_csv("data/sr_hex.csv") %>% 
  # the data without spatial info are not necessary
  filter(!is.na(latitude) & !is.na(longitude))

# load the polygons
cpt_polygons_8 <- st_read("data/city-hex-polygons-8.geojson")

# identify the service requests for Bellville South and count the number of
# requests per polygon to use in the visualisation that follows
belville_south_sr <- sr_hex_df %>% 
  filter(official_suburb == "BELLVILLE SOUTH") %>% 
  rename(index = h3_level8_index) %>% 
  count(index, name = "sr_belville_south") %>% 
  left_join(
    cpt_polygons_8,
    .
  ) 

# Visualise the number of service requests for Bellville South per polygon
belville_south_sr %>% 
  tm_shape() +
  tm_polygons() +
  tm_fill(
    "sr_belville_south", 
    breaks = c(0, 50, 100, 500, 800, 1000, 1200, 1500)
  )
# from the visualisation, it doesn't seem like the polygons age going to enable
# the identification of the centroid. We will have to download the shape file
# to identify the centroid of Bellville South

# download & load the municipal area shape file for cpt using the function 
# sourced at the top of the script (skips download if locally present)
tic("Time taken to load the suburb shape file")
bellville_shp <- load_cpt_suburbs() %>% 
  filter(OFC_SBRB_N == "BELLVILLE SOUTH")
toc()

# select the polygon for Bellville South and determine the centroid
tic("Time taken to derive the centroid of Bellville South")
belville_centroid <- bellville_shp %>% 
  .$geometry %>% 
  st_centroid() 
toc()

# Determine the area which is reachable within 1 min or less from the centroid
tic("Time taken to determine the 1 min reachable area from the centroid")
centroid_buff_1min <- belville_centroid %>% 
  # transform to the appropriate coordinate reference system to attain lat-long
  st_transform(4326) %>% 
  # extract a vector containing the long and lat values for the centroid
  st_coordinates() %>% 
  # use the OSM API to retrieve the area within 1 min from the centroid
  osrmIsochrone(breaks = 1, res = 30) %>% 
  # make sure the polygon is valid for further use
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
tic("Time taken to filter out all points not 1 min from Bellville South centroid")
# The machine I'm working on has limited RAM and implementing in parallel makes
# it spill over into swap memory making the parallel implementation slower than
# the sequential implementation. If you have more memory available, feel free to
# uncomment "for parallel" lines and comment out "comment out to run in 
# parallel" lines

# cl <- makePSOCKcluster(5)  # for parallel
# plan(cluster, workers = cl)  # for parallel
# geom_point_values <- future_map2(  # for parallel
geom_point_values <- map2(  # comment out to run in parallel
  sr_hex_df$longitude,
  sr_hex_df$latitude,
  ~st_point(c(.x, .y))
)

sr_geom_df <- sr_hex_df %>% 
  # it is important to check the CRS of the hex to ensure they match
  st_sf(geometry = geom_point_values, crs = 4326) 

sr_bellville_centroid_1m <- sr_geom_df %>% 
  mutate(
    index = 1:n(),
    index = index %% 10,
    .before = 1
  ) %>% 
  group_nest(index) %>% 
  mutate(
    # geo_filtered = future_map(
    geo_filtered = map(  # comment out to run in parallel
      data, 
      ~st_join(.x, centroid_buff_1min, join = st_within) %>% 
        filter(!is.na(id)) %>% 
        select(-c(id, isomin, isomax))
    )
  ) %>% 
  select(geo_filtered) %>% 
  unnest(geo_filtered) %>% 
  st_as_sf()
# stopCluster(cl)  # for parallel
toc()

sr_bellville_centroid_1m %>% 
  st_as_sf()

adjacent_burbs <- sr_bellville_centroid_1m %>% 
  distinct(official_suburb) %>% 
  pull(official_suburb)

# Display the process
bellville_shp %>% 
  tm_shape() +
  tm_borders() +
  tm_shape(sr_geom_df %>% filter(official_suburb %in% adjacent_burbs)) +
  tm_dots(fill = "green", fill_alpha = 0.3) +
  tm_shape(centroid_buff_1min) +
  tm_polygons(fill_alpha = 0.7, fill = "blue") + 
  tm_layout(frame = FALSE) +
  tm_title("Bellville South outline (black)\nArea within 1min travel (blue)\nLocation of service reports (green)")

# Display the process
bellville_shp %>% 
  tm_shape() +
  tm_borders() +
  tm_shape(centroid_buff_1min) +
  tm_polygons(fill_alpha = 0.7, fill = "blue") + 
  tm_shape(sr_bellville_centroid_1m) +
  tm_dots(fill = "yellow", fill_alpha = 0.3) +
  tm_layout(frame = FALSE) +
  tm_title("Bellville South outline (black)\nArea within 1min travel (blue)\nLocation of service reports in area (yellow)")


# 2. Bring in the wind data
# the link to where the wind data can be downloaded
wind_req_link <- "https://www.capetown.gov.za/_layouts/OpenDataPortalHandler/DownloadHandler.ashx?DocumentName=Wind_direction_and_speed_2020.ods&DatasetDocument=https%3A%2F%2Fcityapps.capetown.gov.za%2Fsites%2Fopendatacatalog%2FDocuments%2FWind%2FWind_direction_and_speed_2020.ods"
wind_file_path <- "data/wind_direction_and_speed_2020.ods"
# download_file is a custom function that downloads a file sourced at the top 
# of the script. Below it is used to download the wind data from the link above
tic("Time taken to download/check wind data file")
download_file(
  location = wind_req_link,
  local_path = wind_file_path
)
toc()

# loading and cleaning the wind data file
bellville_wind_df <-
  read_ods(wind_file_path, skip = 2) %>% 
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

# Join the air quality to the SRs one min from Bellville South centroid 
sr_bellville_wind <- sr_bellville_centroid_1m %>%
  # the weather data is captured at an hourly frequency, create an hourly column
  # that can be used to perform the join
  mutate(
    creation_hour = floor_date(creation_timestamp, unit = "hours"),
    .before = 3
  ) %>% 
  # the frequencies align, we join on them below
  left_join(bellville_wind_df, by = join_by("creation_hour" == "date_time")) 

# 3. anonymise the data
anonymised_bellville_data <- sr_bellville_wind %>% 
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
   h3_level8_index
  ) %>% 
  mutate(
    # calculate the duration from creation to completion
    duration_to_completion_hours = completion_timestamp - creation_timestamp,
    # extract the seconds and convert to hours
    duration_to_completion_hours = (as.numeric(duration_to_completion_hours) / 60) / 60,
    # there are some slight negative values, round to nearest hour to solve
    duration_to_completion_hours = round(duration_to_completion_hours),
    # Use sha256 and add salt to avoid the values being looked up easily 
    hash_id = map_chr(notification_number, ~digest(str_c("_*", as.numeric(.x)^2, "*_"), algo = "sha256")),
    # select a smaller subset of characters (8) for the id
    hash_id = str_sub(hash_id, 1, 8),
    creation_timestamp_6h = floor_date(creation_timestamp, "6H")
  ) %>% 
  # Transform location accuracy to ~500m (could have used hex polygons at level
  # 8 but this methods get exactly 500m approximations)
  # transform the projection to EPSG:32734 to ease working with meters
  # see https://epsg.io/32734 for more information
  st_transform(32734) %>% 
  # generalise to a 500m grid
  st_snap_to_grid(500) %>% 
  # transform back to standard CRS EPSG:4326
  st_transform(4326) %>% 
  # join in the wind data after aggregating it to 6 hour temporal accuracy
  left_join(
    bellville_wind_df %>% 
      # aggregate the wind data to 6 hour temporal accuracy
      mutate(date_time = floor_date(date_time, unit = "6H")) %>% 
      group_by(date_time) %>% 
      summarise(
        across(where(is.double), ~mean(.x, na.rm = T))
      ) %>% 
      # convert the NaNs to NAs, this is more accurate and avoids unexpected
      # behaviour, especially if someone else is going to be working with the 
      # data who might be unaware of their presence. 
      mutate(
        wind_dir_deg = ifelse(is.nan(wind_dir_deg), NA_real_, wind_dir_deg)
      ),
    by = join_by(creation_timestamp_6h == date_time)
  ) %>% 
    select(
      hash_id,
      creation_timestamp_6h,
      duration_to_completion_hours,
      official_suburb,
      directorate,
      department,
      branch,
      section,
      code_group,
      code,
      cause_code,
      # and from the wind data
      wind_speed_m_sec,
      wind_dir_deg
    )

# Print data
anonymised_bellville_data

# Visualise the data
anonymised_bellville_data %>% 
  ggplot(
    aes(x = department, col = department, y = duration_to_completion_hours)
  ) +
  geom_jitter(alpha = 0.3) +
  geom_boxplot(alpha = 0.6) +
  coord_flip() +
  scale_y_continuous(trans = "log") +
  theme_bw() +
  theme(legend.position = "none") +
  labs(
    title = "Time taken to complete service requests",
    x = "",
    y = ""
  )
