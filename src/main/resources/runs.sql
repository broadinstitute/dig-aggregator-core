CREATE TABLE IF NOT EXISTS `runs` (
    `id` INT NOT NULL AUTO_INCREMENT,
    `method` VARCHAR(200) NOT NULL,
    `stage` VARCHAR(200) NOT NULL,
    `input` VARCHAR(1024) NOT NULL,
    `version` DATETIME NOT NULL,
    `output` VARCHAR(200) NOT NULL,
    `timestamp` DATETIME NOT NULL DEFAULT NOW(),
    PRIMARY KEY (`id`),
    UNIQUE INDEX `stage_IDX` (`method` ASC, `stage` ASC, `input` ASC, `output` ASC)
)
