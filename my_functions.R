
### Function 'rank_ct'
# rank_ct takes a dataframe and a column from that dataframe, and counts the
# occurrences of each entry from that column, ranked by occurrences.
# https://dplyr.tidyverse.org/articles/programming.html
rank_ct <- function(df, col) {
  
  col <- enquo(col)
  
  df %>%
    select(!! col) %>%
    group_by(!! col) %>%
    count() %>%
    arrange(desc(n))
}



### Function 'read_loop_df' 
# Reads multiple files from working directory and then binds the rows.  
# Assumes all column headers are identical.
read_loop_df <- function(p) {
  list.files(pattern = p) %>% 
  map_df(~fread(., stringsAsFactors = FALSE))}



### Function 'distinct_counter'

distinct_counter <- function(df){

df %>% purrr::map(., n_distinct) %>% unlist() %>% as.data.frame() %>% rownames_to_column() -> distinct_counts

names(distinct_counts) <- c("col_name", "distinct_ct")

distinct_counts %>% arrange(desc(distinct_ct)) %>% as.tibble()

}


### Function 'distinct_counter'

distinct_na_counter <- function(df){
  
  df %>% purrr::map(., n_distinct) %>% unlist() %>% as.data.frame() %>% rownames_to_column() -> distinct_counts
  
  df %>% purrr::map(., function(col) is.na(col) %>% sum()) %>% unlist() %>% 
    as.data.frame() %>% 
    rownames_to_column() -> 
  na_counts
  
  
  names(distinct_counts) <- c("col_name", "distinct_ct")
  
  names(na_counts) <- c("col_name", "na_ct")
  
  #distinct_counts %>% arrange(desc(distinct_ct)) %>% as.tibble()
  
  #na_counts %>% arrange(desc(na_ct)) %>% as.tibble()
  
  distinct_counts %>% 
    inner_join(na_counts, 
               by=c("col_name"="col_name")) %>% 
    arrange(desc(distinct_ct)) %>% 
    as.tibble() %>% 
    mutate(distinct_pct=distinct_ct/nrow(df)) %>%
    mutate(na_pct=na_ct/nrow(df))
  
}



### Function 'lake_col_names'

lake_col_names <- function(table_name){
  q_func <- 
  paste0(
  "SELECT DISTINCT COLUMN_NAME, COLUMN_TEXT 
  FROM SIGN.SYSCOLUMNS
  WHERE SYSTEM_TABLE_NAME IN (","'",
  table_name,
  "')"
  )
  sqlQuery(lake_connection, q_func)
}








# ##Setup arguments for function....
# 
# # conn
# lk_conn <- "DRIVER=SQL Server Native Client 11.0;SERVER=PRDDLAKESQL01;UID=jpack;Trusted_Connection=Yes;APP=RStudio;WSID=BCP01I03;"  # how are you setting this?
# 
# # q
# Q <- function(item) {
#   paste0(
#   "SELECT TLTACT, TLTAPP, TLTBRN, TLTRCVTMSP 
#   FROM SIGN.TEL2000P_HIST
#   WHERE TLTBRN IN ('", 
#   item, 
#   "')
#   AND TLTCOR NOT IN ('C')
#   AND TLTREJ IN ('000')
#   AND TLTSTS IN ('O')
#   AND TLTCD IN
#   ('0006', '0621', '0722', '0027',
#   'C122', 'C121', '0711', '0011',
#   '073C', '073D', '0012', '073F',
#   '073G', '073H', '073I', '073K',
#   '0010', '0020', '073E', '0015',
#   '073N', '0441', '073P', '073Q',
#   '0411', '0414', '0792', '0791',
#   '073A', '0017', '0713', '0018',
#   '0007', '0218', '0228', '0008',
#   '073N', '073P', '073Q', '0771',
#   '0513', '0512', '0517', '0029',
#   '0518', '0514', '0516', '0515',
#   '0751', '0752', '0753', '0754',
#   'G020', 'G012', 'G73C', 'R11N',
#   'G006', '022N', 'G010', 'G711',
#   'G016', 'G411', 'G22N', '0771',
#   '0411', 'G73C', 'G012', 'G711',
#   'G011', 'G411', '021N', '0411',
#   'G73G', '0782', 'C123', 'C124',
#   'G21N', 'G722', 'G73A', 'G73B',
#   '01', '03', '04', '44', '42', '43',
#   '46', '27', '13', '14', '12', '15',
#   '073', '15', '07', '08', '0218',
#   '0228', '05', '06', '47', '09',
#   '10', '0012', '0016')"
#   )
# }
# 
# # items in set
# branch_numbers <- c(528, 529, 531) %>% as.character()
# # branch_numbers <- read.csv("branch_numbers.csv")
# # branch_numbers <- branch_numbers %>% pull() %>% as.character()
# 
# 
# # p
# p <- function(item) {write_csv(paste0("test_qp_result_", item, ".csv"))} # This line represents the procedure for iteration 'i'.
# 
# 
# ### Function q_p_write_loop
# # Does the function q_p_write_loop(lake_connection, q, p, branch_numbers)
# q_p_write_loop <- function(conn, Q, p, items_in_set){
# 
# source('J:/Stat Model Info/Stat Model Info/Josh Pack/my_libraries.R')
# source('J:/Stat Model Info/Stat Model Info/Josh Pack/conn_datalake.R')
# 
# # query 'qi' >>> This will encompass the query and the procedure for iteration 'i'.  
# q_p_func <- function(i){
#   
#   items_in_set <- items_in_set
#   
#   q <- Q(items_in_set[i])
#   
#   q_result <- sqlQuery(h, q)
#   
#   q_result %>% p(items_in_set[i])
#   
# } #End function "q_p_func"
# 
# 
# # https://stackoverflow.com/questions/38318139/run-a-for-loop-in-parallel-in-r
# # https://stackoverflow.com/questions/41117127/r-parallel-programming-error-in-task-1-failed-could-not-find-function
# # https://stackoverflow.com/questions/41960725/rodbc-foreach
# 
# 
# cores <- detectCores()
# cl <- makeCluster(cores[1]-1)
# registerDoParallel(cl)
# 
# clusterEvalQ(cl, {
#   library(RODBC)
#   connection <- "DRIVER=SQL Server Native Client 11.0;SERVER=PRDDLAKESQL01;UID=jpack;Trusted_Connection=Yes;APP=RStudio;WSID=BCP01I03;"
#   h <- odbcDriverConnect(connection)
# })
# 
# Sys.time()
# tic()
# foreach(i=1:length(items_in_set),
#         .noexport = "h",
#         .packages = c("magrittr", "RODBC", "readr")) %dopar% {q_p_func(i)}
# toc()
# 
# stopCluster(cl)
# 
# } # End of fuction q_p_write_loop
# 
# 
# 
# q_p_write_loop(lk_conn, Q, p, branch_numbers)
# 
# 
# 
# 
# 
# 
# 
# 
# 
# 
# 
# 
# 
# 
# 
# 
# 
# # Sys.time()
# # tic()
# # branch_numbers[1:2] %>% walk(q_func)
# # toc()
# 
# # library(data.table)
# # # > library(data.table)
# # # data.table 1.11.4  Latest news: http://r-datatable.com
# # # 
# # # Attaching package: ‘data.table’
# # # 
# # # The following objects are masked from ‘package:tis’:
# # #   
# # #   between, month, quarter, year
# # # 
# # # The following objects are masked from ‘package:lubridate’:
# # #   
# # #   hour, isoweek, mday, minute, month, quarter, second, wday, week, yday, year
# # # 
# # # The following object is masked from ‘package:purrr’:
# # #   
# # #   transpose
# # # 
# # # The following objects are masked from ‘package:dplyr’:
# # #   
# # #   between, first, last
# # # 
# # # > 
# # 
# # 
# # Sys.time()
# # tic()
# # tbl <-
# #   list.files(pattern = "q_result_") %>% 
# #   map_df(~fread(., stringsAsFactors = FALSE))
# # toc()
# 
# 
