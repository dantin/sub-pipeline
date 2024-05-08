DROP TABLE IF EXISTS `demo`;
CREATE TABLE `demo` (
  `primaryid`         BIGINT       NOT NULL,
  `caseid`            INT          NOT NULL,
  `caseversion`       INT          NULL,
  `i_f_code`          VARCHAR(16)  NULL,
  `event_dt`          DATETIME(6)  NULL,
  `mfr_dt`            DATETIME(6)  NULL,
  `init_fda_dt`       DATETIME(6)  NULL,
  `fda_dt`            DATETIME(6)  NULL,
  `rept_cod`          VARCHAR(16)  NOT NULL,
  `auth_num`          VARCHAR(64)  NOT NULL,
  `mfr_num`           VARCHAR(128) NOT NULL,
  `mfr_sndr`          VARCHAR(128) NOT NULL,
  `lit_ref`           VARCHAR(512) NOT NULL,
  `age`               INT          NOT NULL,
  `age_cod`           CHAR(8)      NOT NULL,
  `age_grp`           CHAR(16)     NOT NULL,
  `sex`               CHAR(8)      NOT NULL,
  `e_sub`             VARCHAR(128) NOT NULL,
  `wt`                DECIMAL(9,3) NOT NULL,
  `wt_cod`            CHAR(8)      NOT NULL,
  `rept_dt`           DATETIME(6)  NULL,
  `to_mfr`            VARCHAR(16)  NOT NULL,
  `occp_cod`          VARCHAR(64)  NOT NULL,
  `reporter_country`  CHAR(32)     NOT NULL,
  `occr_country`      CHAR(16)     NOT NULL
);

DROP TABLE IF EXISTS `drug`;
CREATE TABLE `drug` (
  `primaryid`       BIGINT       NOT NULL,
  `caseid`          INT          NOT NULL,
  `drug_seq`        INT          NOT NULL,
  `role_cod`        VARCHAR(16)  NULL,
  `drugname`        VARCHAR(512) NULL,
  `prod_ai`         VARCHAR(512) NULL,
  `val_vbm`         INT          NOT NULL,
  `route`           VARCHAR(64)  NULL,
  `dose_vbm`        VARCHAR(512) NULL,
  `cum_dose_chr`    CHAR(16)     NOT NULL,
  `cum_dose_unit`   CHAR(8)      NULL,
  `dechal`          VARCHAR(16)  NULL,
  `rechal`          VARCHAR(16)  NULL,
  `lot_num`         VARCHAR(64)  NULL,
  `exp_dt`          DATETIME(6)  NULL,
  `nda_num`         VARCHAR(32)  NULL,
  `dose_amt`        VARCHAR(8)   NULL,
  `dose_unit`       VARCHAR(8)   NULL,
  `dose_form`       VARCHAR(64)  NULL,
  `dose_freq`       VARCHAR(16)  NULL
);

DROP TABLE IF EXISTS `indi`;
CREATE TABLE `indi` (
  `primaryid`       BIGINT       NOT NULL,
  `caseid`          INT          NOT NULL,
  `indi_drug_seq`   INT          NOT NULL,
  `indi_pt`         VARCHAR(128) NULL
);

DROP TABLE IF EXISTS `outc`;
CREATE TABLE `outc` (
  `primaryid`       BIGINT       NOT NULL,
  `caseid`          INT          NOT NULL,
  `outc_cod`        CHAR(8)      NOT NULL
);

DROP TABLE IF EXISTS `reac`;
CREATE TABLE `reac` (
  `primaryid`       BIGINT       NOT NULL,
  `caseid`          INT          NOT NULL,
  `pt`              VARCHAR(256) NOT NULL,
  `drug_rec_act`    VARCHAR(64)  NOT NULL
);

DROP TABLE IF EXISTS `rpsr`;
CREATE TABLE `rpsr` (
  `primaryid`       BIGINT       NOT NULL,
  `caseid`          INT          NOT NULL,
  `rpsr_cod`        CHAR(8)      NOT NULL
);

DROP TABLE IF EXISTS `ther`;
CREATE TABLE `ther` (
  `primaryid`       BIGINT       NOT NULL,
  `caseid`          INT          NOT NULL,
  `dsg_drug_seq`    INT          NOT NULL,
  `start_dt`        DATETIME(6)  NULL,
  `end_dt`          DATETIME(6)  NULL,
  `dur`             VARCHAR(8)   NULL,
  `dur_cod`         CHAR(8)      NULL
);
