library(tidyverse)

wed21 <- 'weddings2021/'
mar21    <- foreign::read.dbf(paste0(wed21, "FD_MAR_2021.dbf"))    |> tibble()
varmod21 <- foreign::read.dbf(paste0(wed21, "varmod_MAR_2021.dbf")) |> tibble()
dict     <- read_csv(paste0(wed21, 'dict.csv'))
dep_dict <- read_csv(paste0(wed21, 'department_dict.csv'))
coverage_dict <- read_csv(paste0(wed21,'coverage_dict.csv'))

# Empirical variable × modality counts (the only intermediate we actually need)
custom_varmod <- mar21 |>
  pivot_longer(cols = everything(), names_to = "COD_VAR", values_to = "COD_MOD") |>
  summarise(n = n(), .by = c(COD_VAR, COD_MOD))

# Metadata combos that never appear in the data
anti_join(varmod21 |> select(COD_VAR, COD_MOD), custom_varmod, by = c("COD_VAR", "COD_MOD"))

# Coverage table
coverage <- varmod21 |>
  distinct(COD_VAR, COD_MOD) |>
  left_join(custom_varmod |> mutate(.found = TRUE), by = c("COD_VAR", "COD_MOD")) |>
  mutate(.found = coalesce(.found, FALSE)) |>
  summarise(
    total_metadata = n(),
    empirical      = sum(.found),
    coverage       = empirical / total_metadata,
    .by = COD_VAR
  ) |>
  left_join(
    dict |> summarise(description = first(description), modalities = list(modalities), .by = variable),
    by = c("COD_VAR" = "variable")
  )

