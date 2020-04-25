#!/mnt/var/install/R-3.6.3/bin/Rscript

w <- 600
h <- 360

# load required libraries
library(grDevices)
library(qqman)
library(calibrate)

# read the table from input source
df <- read.table('associations.tbl', sep='\t', header=TRUE)

# fix chromosomes
df$CHR <- ifelse(df$CHR == 'X', 23, df$CHR)
df$CHR <- ifelse(df$CHR == 'Y', 24, df$CHR)
df$CHR <- ifelse(df$CHR == 'XY', 25, df$CHR)
df$CHR <- ifelse(df$CHR == 'MT', 26, df$CHR)

# target 20,000 data points
target <- 20000 / nrow(df)
points <- df[(! is.na(df$P)) & (df$P > 0) & (df$P <= 1) & (df$P * runif(nrow(df)) < target),]

# generate manhattan plot
png('manhattan.png', width=w, height=h)
manhattan(points)
dev.off()

# generate qq plot
png('qq.png', width=w, height=h)
qq(df$P)
dev.off()

# done
q()
