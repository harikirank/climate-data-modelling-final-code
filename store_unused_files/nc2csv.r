install.packages("ncdf4")

library(ncdf4)
nc_fname <- "daymet_v4_daily_hi_dayl_2022.nc"
nc_ds <- nc_open(nc_fname)

dim_lon <- ncvar_get(nc_ds, "longitude")
dim_lat <- ncvar_get(nc_ds, "latitude")
dim_depth <- ncvar_get(nc_ds, "depth")
dim_time <- ncvar_get(nc_ds, "time")
coords <- as.matrix(expand.grid(dim_lon, dim_lat, dim_depth, dim_time))

var1 <- ncvar_get(nc_ds, "var1", collapse_degen=FALSE)
var2 <- ncvar_get(nc_ds, "var2", collapse_degen=FALSE)

nc_df <- data.frame(cbind(coords, var1, var2))
names(nc_df) <- c("lon", "lat", "depth", "time", "var1", "var2")
head(na.omit(nc_df), 5)  # Display some non-NaN values for a visual check
csv_fname <- "netcdf_filename.csv"
write.table(nc_df, csv_fname, row.names=FALSE, sep=";")
