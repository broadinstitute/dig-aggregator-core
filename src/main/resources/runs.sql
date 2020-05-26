CREATE TABLE IF NOT EXISTS `runs` (
    `id` INT NOT NULL AUTO_INCREMENT,
    `method` VARCHAR(200) NOT NULL,
    `stage` VARCHAR(200) NOT NULL,
    `input` VARCHAR(1024) NOT NULL,
    `version` VARCHAR(80) NOT NULL,
    `output` VARCHAR(200) NOT NULL,
    `timestamp` DATETIME NOT NULL DEFAULT NOW(),
    `source` VARCHAR(1024) NULL,
    `branch` VARCHAR(200) NULL,
    `commit` VARCHAR(40) NULL,
    PRIMARY KEY (`id`),
    UNIQUE INDEX `stage_IDX` (`method` ASC, `stage` ASC, `input` ASC, `output` ASC)
)