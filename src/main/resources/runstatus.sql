CREATE TABLE IF NOT EXISTS `runstatus` (
    `id` INT NOT NULL AUTO_INCREMENT,
    `project` VARCHAR(50) NOT NULL,
    `method` VARCHAR(50) NOT NULL,
    `stage` VARCHAR(50) NOT NULL,
    `output` VARCHAR(250) NOT NULL,
    `started` DATETIME,
    `ended` DATETIME,
    `created` DATETIME NOT NULL DEFAULT NOW(),
    PRIMARY KEY (`id`),
    UNIQUE INDEX `output_IDX` (`project` ASC, `method` ASC, `stage` ASC, `output` ASC)
)
