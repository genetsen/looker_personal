
load_utils <- function(path = "util/R_functions") {
  env <- new.env(parent = globalenv())
  sys.source(path, envir = env, keep.source = FALSE)
  env
}

utils <- load_utils()
# later
utils$slugify("Hi there")
df <- f_gmail_csv_to_dataframe(
  search_criteria = 'prisma__planned_by_month',
  num_results = 10,
  attachment_index = 1,
  validate_date = FALSE,
  filter_na_month = TRUE
)
