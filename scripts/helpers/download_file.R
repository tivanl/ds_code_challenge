download_file <- function(location, local_path){
  
  # check if the file already exists locally
  if(basename(local_path) %in% list.files(dirname(local_path)){
    log_info("The file already exists")
    return(local_path)
  }
  
  # if the file does not exist, download it and return its local location
  log_info("The file does not exist locally, commencing download...")
  request(location) %>% 
    req_perform(path = local_path)
  
  return(local_path)
  
}
