install.packages("readr")
install.packages("tidyr")
install.packages("stringr")
install.packages("dplyr")

library(readr)
library(tidyr)
library(stringr)
library(dplyr)

itmo_authors <- read_csv("itmo_authors_05_22.csv")

scopus_for_works <- read_delim("scopuspubs_itmo_80_22.csv", 
                               "\t", escape_double = FALSE, trim_ws = TRUE)

auth_ids <- scopus_for_works$author_ids
auht_names <- scopus_for_works$author_names
auth_affils <- scopus_for_works$author_afids
eid <- scopus_for_works$eid
date <- scopus_for_works$coverDate

au_df <- cbind.data.frame(eid, auth_ids, auth_affils, date, auht_names)


au_df$org_count <- str_count(au_df$auth_affils, "\\;") + 1
au_df$auth_count <- str_count(au_df$auth_ids, "\\;") + 1
au_df$auth_name_count <- str_count(au_df$auht_names, "\\;") + 1

affil_df_wide <- au_df %>% 
  select(eid, auth_affils) %>% 
  separate(auth_affils,into=as.character(seq(1:max(au_df$org_count))), sep="\\;",extra="merge")

affil_df_long <- affil_df_wide %>% 
  pivot_longer(as.character(1:max(au_df$org_count)), names_to = "org_pos_in_article", values_to = "orgs_ids") %>% 
  na.omit()

auth_df_wide <- au_df %>% 
  select(eid, auth_ids) %>% 
  separate(auth_ids,into=as.character(seq(1:max(au_df$auth_count))), sep="\\;",extra="merge")

auth_df_long <- auth_df_wide %>% 
  pivot_longer(as.character(1:max(au_df$auth_count)), names_to = "auth_pos_in_article", values_to = "auth_ids") %>% 
  na.omit()




au_af_df <- auth_df_long %>% 
  bind_cols(affil_df_long)

au_af_unique <- au_af_df %>% 
  mutate(eid = eid...1) %>% 
  select(eid, auth_ids, orgs_ids) %>% 
  distinct()

au_af_unique$org_count <- str_count(au_af_unique$orgs_ids, "\\-") + 1

testing_df <- au_af_unique %>% 
  group_by(eid) %>% 
  count()

au_af_unique <- au_af_unique %>% 
  left_join(testing_df, by = "eid", keep = FALSE) %>% 
  rename(author_count = 5) %>% 
  mutate(share_test = (1/org_count)*(1/author_count))

itmo_authors <- itmo_authors %>% 
  rename(author_name = Автор, auth_ids = `Идентификтор автора`) %>% 
  select(author_name, auth_ids) %>% 
  mutate(auth_ids = as.character(auth_ids))

au_af_unique_itmo <- au_af_unique %>% 
  mutate(itmo = str_count(orgs_ids, "60072485")) %>% 
  filter(itmo >= 1) %>% 
  group_by_at(vars(auth_ids)) %>% 
  summarise(auth_share_sum = sum(share_test), auth_pubs_count = n()) %>% 
  left_join(itmo_authors, by = "auth_ids", keep= FALSE) %>% 
  arrange(desc(auth_share_sum))

write.csv(au_af_unique_itmo, "itmo_authors_grouped.csv", fileEncoding = "UTF-8")


au_af_unique$au_org <- paste0(au_af_unique$auth_ids, " — " ,au_af_unique$orgs_ids, sep="")

grouped_smt <- aggregate(au_org ~ eid, data = au_af_unique, FUN = paste, collapse = "; ")

scopus_for_works_fin <- merge(scopus_for_works, grouped_smt, by = "eid", all.x = TRUE)

write.csv(scopus_for_works_fin, "scopus_itmo_80_22_authors.csv", fileEncoding = "UTF-8")
