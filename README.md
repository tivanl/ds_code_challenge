# J-PAL & CCT Data Engineer Code Challenge
Tivan
2025-08-20

<div align="center">

<img src="img/jpal_logo.png" alt="J-PAL logo" width="200"/>
<img src="img/city_emblem.png" alt="City emblem" width="200"/>

</div>

# Introduction

The purpose of this document is to walk through the my solutions to the
[Code
Challenge](https://github.com/cityofcapetown/ds_code_challenge/tree/j-pal-data-engineer)
for the Data Engineer position at J-PAL Africa. The solutions are
divided into three sections, each with a corresponding script.
<a href="#sec-extraction" class="quarto-xref">Section 2.1</a>
corresponds to `1_s3_select.R` which retrieves data from AWS,
<a href="#sec-initial" class="quarto-xref">Section 2.2</a> corresponds
to `2_initial_data_transformation.R` which combines uses spatial joins
to combine two datasets, and
<a href="#sec-further" class="quarto-xref">Section 2.3</a> corresponds
to `3_further_data_transformation.R` which applies spatial filtering,
adds additional data to enrich the dataset, and retrieves additional
data where necessary.

# Challenge Solutions

The packages below need to be installed to enable the code in the
scripts. There are some functions that use `system()` to make calls
directly to the operating system. The code was written using Linux.

``` r
# # a vector containing the names of all the packages used in the document
# pkg <- c(
#   "paws", 
#   "httr2", 
#   "tictoc", 
#   "logger", 
#   "glue",
#   "sf", 
#   "jsonlite", 
#   "tmap", 
#   "furrr", 
#   "parallel",
#   "janitor", 
#   "osrm", 
#   "readODS", 
#   "tidyverse"
# )
# # vector indicating whether the packages in pkg are installed
# filter_vec <- !pkg %in% installed.packages()[,"Package"]
# # install those that have not been installed
# if(sum(filter_vec) > 0) walk(pkg[filter_vec], ~install.packages(.x, Ncpus = 5))
# 
# library(paws)
# library(httr2)
# library(tictoc)
# library(logger)
# library(glue)
# library(sf)
# library(jsonlite)
# library(tmap)
# library(furrr)
# library(parallel)
# library(janitor)
# library(glue)
# library(osrm)
# library(readODS)
# library(tidyverse)
```

Besides the packages read in above, I also define custom function which
can be found in `scripts/helpers`. I source them in the code chunk
below.

``` r
# source("scripts/helpers/download_cpt_s3_data.R")
# source("scripts/helpers/st_join_contains.R")
# source("scripts/helpers/load_cpt_suburbs.R")
```

## Data extraction

## Initial Data Transformation

## Further Data Transformation
