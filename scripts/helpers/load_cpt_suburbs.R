# this function downloads the CPT Official Suburbs shape file if it does not 
# exist, loads it in and returns it as the output of the function
load_cpt_suburbs <- function(){
  
  # check if the zip file has already been retrieved
  if("Official_Planning_Suburbs.zip" %in% list.files("data/")){
    
    log_info("The file already exists, no need to download")
    
    # if it has been retrieved, check that it has been extracted
    if(length(list.files("data/Official_Planning_Suburbs/")) < 6){
      
      log_info("The directory is being extracted")
      
      system("unzip data/Official_Planning_Suburbs.zip -d data/Official_Planning_Suburbs")
      
    }
    
    log_info("Loading shape file")
    # if it has, read it, and return it as the function output
    cpt_suburb_shp <- st_read("data/Official_Planning_Suburbs/Official_Planning_Suburbs.shp")
    
    return(cpt_suburb_shp)
  }
  
  log_info("The file does not exist, proceeding to request it from the web")
  
  # request the Official Planning Suburbs shape file & save it locally
  request("https://hub.arcgis.com/api/v3/datasets/8ebcd15badfe40a4ab759682aacf8439_3/downloads/data?format=shp&spatialRefId=3857&where=1%3D1") %>% 
    req_perform("data/Official_Planning_Suburbs.zip")
  
  # unzip the folder & save it in the data directory
  if(last_response() %>% resp_status() == 200){
    
    log_info("The directory is being extracted")
    system("unzip data/Official_Planning_Suburbs.zip -d data/Official_Planning_Suburbs")
    
  }
  
  log_info("Loading shape file")
  # read in the shape file
  cpt_suburb_shp <- st_read("data/Official_Planning_Suburbs/Official_Planning_Suburbs.shp")
  
  return(cpt_suburb_shp)
  
}