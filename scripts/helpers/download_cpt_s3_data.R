# define a function for retrieving a file using its link
download_cpt_s3_data <- function(file_location){
  
  # get the file name
  file_name <- basename(file_location)
  # define a local path to save the file to (or which it has been saved to)
  local_path <- glue("data/{file_name}")
  
  # check if the file already exists
  if(file_name %in% list.files("data")){
    log_info("The file already exists")
    return(local_path)
  }
  
  log_info("The file does not exist and is being downloaded...")
  
  # create http request object
  req_sr_hex <- request(file_location)
  # perform the request and save the result to the local path
  resp <- req_sr_hex %>% 
    req_perform(path = local_path)
  
  # unzip the file if it was zipped
  if(grepl("\\.gz$", file_name)){
    local_path = gsub("\\.gz$", "", local_path)
    system(glue("gunzip -k data/{file_name}"))
  }
  
  return(local_path)
  
}