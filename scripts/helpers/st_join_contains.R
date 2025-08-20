# define a function to perform spatial joins using the st_contains algorithm
# and keeps only the h3 level 8 index and the notification_number (unique ID)
st_join_contains <- function(points_df, polygons_df){
  
  poly_point_join <- st_join(
    x = polygons_df,
    y = points_df,
    join = st_contains
  ) %>% 
    st_drop_geometry() %>% 
    as_tibble() %>% 
    select(
      h3_level8_index = index,
      notification_number
    ) %>% 
    # we don't want to keep redundant polygons (ones with no sr info associated)
    filter(!is.na(notification_number))
  
  return(poly_point_join)
  
}