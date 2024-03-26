CREATE TABLE IF NOT EXISTS `runs` (
    `id` INT NOT NULL AUTO_INCREMENT,
    `project` VARCHAR(200) NOT NULL,
    `method` VARCHAR(200) NOT NULL,
    `stage` VARCHAR(200) NOT NULL,
    `input` VARCHAR(1024) NOT NULL,
    `version` DATETIME NOT NULL,
    `output` VARCHAR(200) NOT NULL,
    `timestamp` DATETIME NOT NULL DEFAULT NOW(),
    PRIMARY KEY (`id`),
    UNIQUE INDEX `stage_IDX` (`project` ASC, `method` ASC, `stage` ASC, `input` ASC, `output` ASC)
)
