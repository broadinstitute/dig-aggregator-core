#!/mnt/var/install/R-4.0.0/bin/Rscript

w <- 800
h <- 400

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

# ensure the chromosome is numeric
df$CHR <- as.numeric(df$CHR)

# create a column to filter data points with
df$R <- runif(nrow(df))

# target ~50,000 data points
target <- 50000 / nrow(df)

# filter manhattan plot points
points <- df[is.finite(df$P) & (df$P > 0) & (df$P <= 1) & (df$P * df$R < target),]

# write the data points used so it can be debugged
write.table(points, 'manhattan.tbl', quote=FALSE, sep='\t', row.names=FALSE)

# generate manhattan plot
png('manhattan.png', width=w, height=h)
manhattan(points)
dev.off()

# generate qq plot
png('qq.png', width=w, height=h)
par(bty='L')
qq(df$P)
dev.off()

# done
q()
