-- MySQL dump 10.13  Distrib 5.7.30, for Linux (x86_64)
--
-- Host: localhost    Database: bse
-- ------------------------------------------------------
-- Server version	5.7.30-0ubuntu0.18.04.1

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `annual_report`
--

DROP TABLE IF EXISTS `annual_report`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `annual_report` (
  `year` varchar(10) DEFAULT NULL,
  `file_name` varchar(100) DEFAULT NULL,
  `dt_tm` varchar(25) DEFAULT NULL,
  `scrip_code` varchar(25) DEFAULT NULL,
  `created_at` datetime DEFAULT NULL,
  `modified_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY `file_name` (`file_name`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `block_deals`
--

DROP TABLE IF EXISTS `block_deals`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `block_deals` (
  `client_name` varchar(100) DEFAULT NULL,
  `deal_date` varchar(50) DEFAULT NULL,
  `price` varchar(25) DEFAULT NULL,
  `quantity` varchar(25) DEFAULT NULL,
  `scrip_code` varchar(25) NOT NULL,
  `transaction_type` varchar(10) DEFAULT NULL,
  `scripname` varchar(200) DEFAULT NULL,
  `created_at` datetime DEFAULT NULL,
  `modified_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY `scrip_code` (`scrip_code`,`client_name`,`quantity`,`deal_date`),
  UNIQUE KEY `client_name` (`client_name`,`quantity`,`deal_date`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `board_meeting`
--

DROP TABLE IF EXISTS `board_meeting`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `board_meeting` (
  `long_name` varchar(200) DEFAULT NULL,
  `purpose_name` varchar(200) DEFAULT NULL,
  `short_name` varchar(200) DEFAULT NULL,
  `meeting_date` varchar(50) DEFAULT NULL,
  `scrip_code` varchar(25) NOT NULL,
  `tm` varchar(50) DEFAULT NULL,
  `created_at` datetime DEFAULT NULL,
  `modified_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY `scrip_code` (`scrip_code`,`purpose_name`,`meeting_date`),
  UNIQUE KEY `long_name` (`long_name`,`purpose_name`,`meeting_date`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `bse_crawl`
--

DROP TABLE IF EXISTS `bse_crawl`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `bse_crawl` (
  `security_code` varchar(100) COLLATE utf8_unicode_ci NOT NULL,
  `security_name` varchar(255) COLLATE utf8_unicode_ci NOT NULL,
  `crawl_status` int(11) NOT NULL DEFAULT '0',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `modified_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`security_code`,`crawl_status`),
  UNIQUE KEY `security_code` (`security_code`,`crawl_status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `bulk_deals`
--

DROP TABLE IF EXISTS `bulk_deals`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `bulk_deals` (
  `deal_date` varchar(25) DEFAULT NULL,
  `scrip_code` varchar(25) NOT NULL,
  `scripname` varchar(100) DEFAULT NULL,
  `client_name` varchar(300) DEFAULT NULL,
  `transaction_type` varchar(25) DEFAULT NULL,
  `quantity` varchar(50) DEFAULT NULL,
  `price` varchar(25) DEFAULT NULL,
  `created_at` datetime DEFAULT NULL,
  `modified_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY `scrip_code` (`scrip_code`,`client_name`,`quantity`,`deal_date`),
  UNIQUE KEY `client_name` (`client_name`,`quantity`,`deal_date`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `consolidated_pledge`
--

DROP TABLE IF EXISTS `consolidated_pledge`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `consolidated_pledge` (
  `companyName` varchar(100) DEFAULT NULL,
  `flag_pledge` varchar(50) DEFAULT NULL,
  `f_newcol` varchar(25) DEFAULT NULL,
  `fld_enddate` varchar(25) DEFAULT NULL,
  `fld_quarterid` varchar(25) DEFAULT NULL,
  `noofshares_total_promoter_holding` varchar(100) DEFAULT NULL,
  `noofsharespledged` varchar(200) DEFAULT NULL,
  `promoterencum_noofshares` varchar(100) DEFAULT NULL,
  `promoterencum_percof_promotershares` varchar(100) DEFAULT NULL,
  `promoterencum_percof_totalshares` varchar(100) DEFAULT NULL,
  `percentage_total_promoter_holding` varchar(100) DEFAULT NULL,
  `public_noofshares_holding` varchar(100) DEFAULT NULL,
  `shp_pulishedtime` varchar(25) DEFAULT NULL,
  `srno` varchar(50) DEFAULT NULL,
  `scripcode` varchar(25) NOT NULL,
  `total_no_of_issued_shares` varchar(50) DEFAULT NULL,
  `totalnoofshares` varchar(50) DEFAULT NULL,
  `ddateend` varchar(25) DEFAULT NULL,
  `sqtrname` varchar(100) DEFAULT NULL,
  `scrip_code` varchar(25) NOT NULL,
  `created_at` datetime DEFAULT NULL,
  `modified_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`scripcode`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `corp_action`
--

DROP TABLE IF EXISTS `corp_action`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `corp_action` (
  `amount` varchar(20) DEFAULT NULL,
  `bcrd_from` varchar(200) DEFAULT NULL,
  `purpose_name` varchar(200) DEFAULT NULL,
  `scrip_code` varchar(25) NOT NULL,
  `created_at` datetime DEFAULT NULL,
  `modified_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY `bcrd_from` (`bcrd_from`,`purpose_name`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `corp_annexure_1`
--

DROP TABLE IF EXISTS `corp_annexure_1`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `corp_annexure_1` (
  `fld_affirmationLblid` varchar(200) DEFAULT NULL,
  `fld_affirmationlblname` varchar(200) DEFAULT NULL,
  `fld_appoinmentdate` varchar(50) DEFAULT NULL,
  `fld_authorisedate` varchar(50) DEFAULT NULL,
  `fld_bodcompid` varchar(125) DEFAULT NULL,
  `fld_bodmeetingid` varchar(125) DEFAULT NULL,
  `fld_category1` varchar(150) DEFAULT NULL,
  `fld_category2` varchar(125) DEFAULT NULL,
  `fld_categoryfinal` varchar(25) DEFAULT NULL,
  `fld_cessationdate` varchar(25) DEFAULT NULL,
  `fld_committeecompid` varchar(50) DEFAULT NULL,
  `fld_compdetails` varchar(300) DEFAULT NULL,
  `fld_compstatus` varchar(200) DEFAULT NULL,
  `fld_din` varchar(200) DEFAULT NULL,
  `fld_dinreason` varchar(300) DEFAULT NULL,
  `fld_dob` varchar(30) DEFAULT NULL,
  `fld_independantdirnum` varchar(300) DEFAULT NULL,
  `fld_isrequirquorummet` varchar(200) DEFAULT NULL,
  `fld_mobnotes` varchar(300) DEFAULT NULL,
  `fld_masterid` varchar(50) DEFAULT NULL,
  `fld_maxgap` varchar(300) DEFAULT NULL,
  `fld_meetingdatepq` varchar(50) DEFAULT NULL,
  `fld_meetingdaterq` varchar(500) DEFAULT NULL,
  `fld_membername` varchar(100) DEFAULT NULL,
  `fld_nameofothercommittee` varchar(250) DEFAULT NULL,
  `fld_nameofcommittee` varchar(150) DEFAULT NULL,
  `fld_nameofdirectors` varchar(200) DEFAULT NULL,
  `fld_noofchairperson` varchar(200) DEFAULT NULL,
  `fld_noofdirectorship` varchar(100) DEFAULT NULL,
  `fld_noofinddirectorship` varchar(300) DEFAULT NULL,
  `fld_noofmembership` varchar(200) DEFAULT NULL,
  `fld_notes` varchar(200) DEFAULT NULL,
  `fld_pan` varchar(50) DEFAULT NULL,
  `fld_presentdirnum` varchar(100) DEFAULT NULL,
  `fld_quarterid` varchar(50) DEFAULT NULL,
  `fld_reappoinmentdate` varchar(50) DEFAULT NULL,
  `fld_regulationno` varchar(50) DEFAULT NULL,
  `fld_salutation` varchar(100) DEFAULT NULL,
  `fld_scripcode` varchar(100) DEFAULT NULL,
  `fld_sequence` varchar(100) DEFAULT NULL,
  `fld_tenure` varchar(100) DEFAULT NULL,
  `scrip_code` varchar(25) DEFAULT NULL,
  `created_at` datetime DEFAULT NULL,
  `modified_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY `fld_bodcompid` (`fld_bodcompid`),
  UNIQUE KEY `fld_quarterid` (`fld_quarterid`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `corp_annexure_2`
--

DROP TABLE IF EXISTS `corp_annexure_2`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `corp_annexure_2` (
  `fld_affirmationlblid` varchar(200) DEFAULT NULL,
  `fld_affirmationlblname` varchar(200) DEFAULT NULL,
  `fld_appoinmentdate` varchar(50) DEFAULT NULL,
  `fld_authorisedate` varchar(50) DEFAULT NULL,
  `fld_bodcompid` varchar(125) DEFAULT NULL,
  `fld_bodmeetingid` varchar(125) DEFAULT NULL,
  `fld_category1` varchar(150) DEFAULT NULL,
  `fld_category2` varchar(125) DEFAULT NULL,
  `fld_categoryfinal` varchar(25) DEFAULT NULL,
  `fld_cessationdate` varchar(25) DEFAULT NULL,
  `fld_committeecompid` varchar(50) DEFAULT NULL,
  `fld_compdetails` varchar(300) DEFAULT NULL,
  `fld_compstatus` varchar(200) DEFAULT NULL,
  `fld_din` varchar(200) DEFAULT NULL,
  `fld_dinreason` varchar(300) DEFAULT NULL,
  `fld_dob` varchar(30) DEFAULT NULL,
  `fld_independantdirnum` varchar(300) DEFAULT NULL,
  `fld_isrequirquorummet` varchar(200) DEFAULT NULL,
  `fld_mobnotes` varchar(300) DEFAULT NULL,
  `fld_masterid` varchar(50) DEFAULT NULL,
  `fld_maxgap` varchar(300) DEFAULT NULL,
  `fld_meetingdatepq` varchar(50) DEFAULT NULL,
  `fld_meetingDaterq` varchar(500) DEFAULT NULL,
  `fld_membername` varchar(100) DEFAULT NULL,
  `fld_nameofothercommittee` varchar(250) DEFAULT NULL,
  `fld_nameofcommittee` varchar(150) DEFAULT NULL,
  `fld_nameofdirectors` varchar(200) DEFAULT NULL,
  `fld_noofchairperson` varchar(200) DEFAULT NULL,
  `fld_noofdirectorship` varchar(100) DEFAULT NULL,
  `fld_noofinddirectorship` varchar(300) DEFAULT NULL,
  `fld_noofmembership` varchar(200) DEFAULT NULL,
  `fld_notes` varchar(200) DEFAULT NULL,
  `fld_pan` varchar(50) DEFAULT NULL,
  `fld_presentdirnum` varchar(100) DEFAULT NULL,
  `fld_quarterid` varchar(50) DEFAULT NULL,
  `fld_reappoinmentdate` varchar(50) DEFAULT NULL,
  `fld_regulationno` varchar(50) DEFAULT NULL,
  `fld_salutation` varchar(100) DEFAULT NULL,
  `fld_scripcode` varchar(100) DEFAULT NULL,
  `fld_sequence` varchar(100) DEFAULT NULL,
  `fld_tenure` varchar(100) DEFAULT NULL,
  `created_at` datetime DEFAULT NULL,
  `modified_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY `fld_bodcompid` (`fld_bodcompid`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `corp_announcement`
--

DROP TABLE IF EXISTS `corp_announcement`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `corp_announcement` (
  `agenda_id` varchar(200) DEFAULT NULL,
  `announcement_type` varchar(200) DEFAULT NULL,
  `attachmentname` varchar(250) DEFAULT NULL,
  `categoryname` varchar(300) DEFAULT NULL,
  `criticalnews` varchar(25) DEFAULT NULL,
  `dt_tm` varchar(125) DEFAULT NULL,
  `dissemdt` varchar(50) DEFAULT NULL,
  `filestatus` varchar(125) DEFAULT NULL,
  `headline` varchar(125) DEFAULT NULL,
  `more` varchar(125) DEFAULT NULL,
  `newsid` varchar(25) DEFAULT NULL,
  `newssub` varchar(200) DEFAULT NULL,
  `news_dt` varchar(200) DEFAULT NULL,
  `nsurl` varchar(300) DEFAULT NULL,
  `news_submission_dt` varchar(300) DEFAULT NULL,
  `old` varchar(300) DEFAULT NULL,
  `pdfflag` varchar(300) DEFAULT NULL,
  `quarter_id` varchar(200) DEFAULT NULL,
  `rn` varchar(300) DEFAULT NULL,
  `scrip_cd` varchar(200) DEFAULT NULL,
  `slongname` varchar(200) DEFAULT NULL,
  `timediff` varchar(50) DEFAULT NULL,
  `totalpagecnt` varchar(100) DEFAULT NULL,
  `xml_name` varchar(300) DEFAULT NULL,
  `created_at` datetime DEFAULT NULL,
  `modified_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY `newsid` (`newsid`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `corp_info`
--

DROP TABLE IF EXISTS `corp_info`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `corp_info` (
  `sdesignation` varchar(200) DEFAULT NULL,
  `sfirstname` varchar(200) DEFAULT NULL,
  `slastname` varchar(200) DEFAULT NULL,
  `smiddlename` varchar(200) DEFAULT NULL,
  `sprefix` varchar(25) DEFAULT NULL,
  `scrip_code` varchar(25) NOT NULL,
  `created_at` datetime DEFAULT NULL,
  `modified_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY `sfirstname` (`sfirstname`,`slastname`,`sdesignation`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `debt`
--

DROP TABLE IF EXISTS `debt`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `debt` (
  `change_` varchar(100) DEFAULT NULL,
  `dt_tm` varchar(50) DEFAULT NULL,
  `highrate` varchar(25) DEFAULT NULL,
  `isin_number` varchar(50) DEFAULT NULL,
  `instrument` varchar(125) DEFAULT NULL,
  `lowrate` varchar(100) DEFAULT NULL,
  `ltradert` varchar(200) DEFAULT NULL,
  `openrate` varchar(100) DEFAULT NULL,
  `scripname` varchar(100) DEFAULT NULL,
  `turnover` varchar(100) DEFAULT NULL,
  `volume` varchar(100) DEFAULT NULL,
  `ytm` varchar(100) DEFAULT NULL,
  `change_percent` varchar(125) DEFAULT NULL,
  `scrip_cd` varchar(50) DEFAULT NULL,
  `scrip_code` varchar(25) DEFAULT NULL,
  `created_at` datetime DEFAULT NULL,
  `modified_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY `scrip_cd` (`scrip_cd`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `insider_1992`
--

DROP TABLE IF EXISTS `insider_1992`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `insider_1992` (
  `broadcastdttm` varchar(100) DEFAULT NULL,
  `buy_sell` varchar(50) DEFAULT NULL,
  `company_name` varchar(200) DEFAULT NULL,
  `fld_mode` varchar(150) DEFAULT NULL,
  `flag` varchar(50) DEFAULT NULL,
  `fld_acqsolddatefrom` varchar(100) DEFAULT NULL,
  `fld_acqsolddateto` varchar(200) DEFAULT NULL,
  `insider_name` varchar(100) DEFAULT NULL,
  `modified_date` varchar(100) DEFAULT NULL,
  `perc_after` varchar(100) DEFAULT NULL,
  `perc_of_buysell` varchar(100) DEFAULT NULL,
  `quantity` varchar(100) DEFAULT NULL,
  `quantity_after` varchar(125) DEFAULT NULL,
  `scrip_code` varchar(25) NOT NULL,
  `transaction_date` varchar(50) DEFAULT NULL,
  `created_at` datetime DEFAULT NULL,
  `modified_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY `modified_date` (`modified_date`,`insider_name`,`fld_acqsolddatefrom`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `insider_2015`
--

DROP TABLE IF EXISTS `insider_2015`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `insider_2015` (
  `companyname` varchar(200) DEFAULT NULL,
  `fld_attachmenticra` varchar(200) DEFAULT NULL,
  `fld_authorisedate` varchar(50) DEFAULT NULL,
  `fld_contractspecifications` varchar(300) DEFAULT NULL,
  `fld_createdate` varchar(25) DEFAULT NULL,
  `fld_dateintimation` varchar(25) DEFAULT NULL,
  `fld_exportflag` varchar(50) DEFAULT NULL,
  `fld_fromdate` varchar(25) DEFAULT NULL,
  `fld_fromdateallotment` varchar(25) DEFAULT NULL,
  `fld_id` varchar(25) DEFAULT NULL,
  `fld_letterdate` varchar(25) DEFAULT NULL,
  `fld_mode` varchar(200) DEFAULT NULL,
  `fld_modeofacquisition` varchar(200) DEFAULT NULL,
  `fld_notes` text,
  `fld_percentofshareholdingpost` varchar(300) DEFAULT NULL,
  `fld_percentofshareholdingpre` varchar(300) DEFAULT NULL,
  `fld_personcatgname` varchar(300) DEFAULT NULL,
  `fld_promoteradd` varchar(200) DEFAULT NULL,
  `fld_promotercin` varchar(30) DEFAULT NULL,
  `fld_promotercatg` varchar(200) DEFAULT NULL,
  `fld_promotercontact` varchar(300) DEFAULT NULL,
  `fld_promoterdin` varchar(100) DEFAULT NULL,
  `fld_promotername` varchar(500) DEFAULT NULL,
  `fld_promoterpan` varchar(100) DEFAULT NULL,
  `fld_scripcode` varchar(50) NOT NULL,
  `fld_securityno` varchar(50) DEFAULT NULL,
  `fld_securitynopost` varchar(200) DEFAULT NULL,
  `fld_securitynoprior` varchar(200) DEFAULT NULL,
  `fld_securitytype` varchar(100) DEFAULT NULL,
  `fld_securitytypename` varchar(300) DEFAULT NULL,
  `fld_securitytypenameprior` varchar(200) DEFAULT NULL,
  `fld_securitytypepost` varchar(200) DEFAULT NULL,
  `fld_securitytypeprior` varchar(200) DEFAULT NULL,
  `fld_securityvalue` varchar(200) DEFAULT NULL,
  `fld_stampdate` varchar(50) DEFAULT NULL,
  `fld_todate` varchar(50) DEFAULT NULL,
  `fld_todateallotment` varchar(50) DEFAULT NULL,
  `fld_tradederivbuyunits` varchar(100) DEFAULT NULL,
  `fld_tradederivbuyvalue` varchar(100) DEFAULT NULL,
  `fld_tradederivsellunits` varchar(100) DEFAULT NULL,
  `fld_tradederivsellvalue` varchar(100) DEFAULT NULL,
  `fld_tradeexchange` varchar(100) DEFAULT NULL,
  `fld_transactiontype` varchar(100) DEFAULT NULL,
  `fld_typeofcontract` varchar(100) DEFAULT NULL,
  `fld_updatedate` varchar(50) DEFAULT NULL,
  `fld_attachment` varchar(300) DEFAULT NULL,
  `modeofaquisation` varchar(300) DEFAULT NULL,
  `created_at` datetime DEFAULT NULL,
  `modified_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY `fld_id` (`fld_id`,`fld_scripcode`,`fld_promoterpan`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `insider_sast`
--

DROP TABLE IF EXISTS `insider_sast`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `insider_sast` (
  `acq_sale` varchar(100) DEFAULT NULL,
  `acq_sale_pct` varchar(100) DEFAULT NULL,
  `acq_sale_qty` varchar(100) DEFAULT NULL,
  `acquisition_after` varchar(150) DEFAULT NULL,
  `acquisition_pct_after` varchar(50) DEFAULT NULL,
  `acquisition_date` varchar(100) DEFAULT NULL,
  `company_name` varchar(200) DEFAULT NULL,
  `fld_mode` varchar(100) DEFAULT NULL,
  `fld_acqsolddatefrom` varchar(100) DEFAULT NULL,
  `fld_nameofpg` varchar(100) DEFAULT NULL,
  `fld_totdilperafteracq` varchar(100) DEFAULT NULL,
  `newdt` varchar(100) DEFAULT NULL,
  `flag` varchar(125) DEFAULT NULL,
  `modify_date` varchar(50) DEFAULT NULL,
  `ord` varchar(100) DEFAULT NULL,
  `scrip_code` varchar(25) DEFAULT NULL,
  `shareholdername` varchar(200) DEFAULT NULL,
  `created_at` datetime DEFAULT NULL,
  `modified_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY `bse_sast` (`scrip_code`,`shareholdername`,`fld_acqsolddatefrom`,`acq_sale`,`acq_sale_pct`,`acq_sale_qty`,`acquisition_after`,`acquisition_pct_after`,`modify_date`,`ord`,`newdt`,`flag`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `notice`
--

DROP TABLE IF EXISTS `notice`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `notice` (
  `notice_no` varchar(100) NOT NULL,
  `subject` varchar(200) DEFAULT NULL,
  `subject_link` varchar(250) DEFAULT NULL,
  `segmentname` varchar(125) DEFAULT NULL,
  `categoryname` varchar(125) DEFAULT NULL,
  `departmentname` varchar(125) DEFAULT NULL,
  `scrip_code` varchar(25) NOT NULL,
  `created_at` datetime DEFAULT NULL,
  `modified_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`notice_no`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `peer`
--

DROP TABLE IF EXISTS `peer`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `peer` (
  `bv` varchar(200) DEFAULT NULL,
  `bodies_corporate` varchar(200) DEFAULT NULL,
  `cash_eps` varchar(150) DEFAULT NULL,
  `change_` varchar(300) DEFAULT NULL,
  `dii` varchar(125) DEFAULT NULL,
  `eps` varchar(125) DEFAULT NULL,
  `equity` varchar(150) DEFAULT NULL,
  `face_value` varchar(125) DEFAULT NULL,
  `fii` varchar(125) DEFAULT NULL,
  `foreign_` varchar(125) DEFAULT NULL,
  `indian` varchar(25) DEFAULT NULL,
  `institution` varchar(200) DEFAULT NULL,
  `ltp` varchar(200) DEFAULT NULL,
  `npm` varchar(300) DEFAULT NULL,
  `name` varchar(300) DEFAULT NULL,
  `nonins` varchar(300) DEFAULT NULL,
  `opm` varchar(300) DEFAULT NULL,
  `pat` varchar(200) DEFAULT NULL,
  `pb` varchar(30) DEFAULT NULL,
  `pe` varchar(200) DEFAULT NULL,
  `pnpgrp` varchar(300) DEFAULT NULL,
  `public` varchar(100) DEFAULT NULL,
  `ronw` varchar(500) DEFAULT NULL,
  `resqtr` varchar(100) DEFAULT NULL,
  `results_quartername` varchar(150) DEFAULT NULL,
  `revenue` varchar(50) DEFAULT NULL,
  `shp_quartername` varchar(200) DEFAULT NULL,
  `scrip_cd` varchar(200) DEFAULT NULL,
  `w52hi` varchar(100) DEFAULT NULL,
  `w52hidt` varchar(100) DEFAULT NULL,
  `w52lo` varchar(100) DEFAULT NULL,
  `w52lodt` varchar(200) DEFAULT NULL,
  `scrip_code` varchar(25) DEFAULT NULL,
  `created_at` datetime DEFAULT NULL,
  `modified_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY `scrip_cd` (`scrip_cd`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `results_annual`
--

DROP TABLE IF EXISTS `results_annual`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `results_annual` (
  `scrip_code` varchar(25) NOT NULL,
  `json_value` text,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`scrip_code`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `results_qrt`
--

DROP TABLE IF EXISTS `results_qrt`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `results_qrt` (
  `scrip_code` varchar(25) NOT NULL,
  `json_value` text,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`scrip_code`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `sast_annual`
--

DROP TABLE IF EXISTS `sast_annual`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `sast_annual` (
  `cs_fld_no_of_share` varchar(200) DEFAULT NULL,
  `cs_fld_per_dilutedshare` varchar(200) DEFAULT NULL,
  `cs_fld_per_share` varchar(150) DEFAULT NULL,
  `fld_exchangename` varchar(300) DEFAULT NULL,
  `fld_fyear` varchar(25) DEFAULT NULL,
  `fld_name` varchar(125) DEFAULT NULL,
  `fld_scripcode` varchar(50) NOT NULL,
  `oi_fld_no_of_share` varchar(125) DEFAULT NULL,
  `oi_fld_per_dilutedshare` varchar(125) DEFAULT NULL,
  `oi_fld_per_share` varchar(125) DEFAULT NULL,
  `sh_fld_no_of_share` varchar(25) DEFAULT NULL,
  `sh_fld_per_dilutedshare` varchar(200) DEFAULT NULL,
  `sh_fld_per_share` varchar(200) DEFAULT NULL,
  `vr_fld_no_of_share` varchar(300) DEFAULT NULL,
  `vr_fld_per_dilutedshare` varchar(300) DEFAULT NULL,
  `vr_fld_per_share` varchar(300) DEFAULT NULL,
  `wr_fld_no_of_share` varchar(300) DEFAULT NULL,
  `wr_fld_per_dilutedshare` varchar(200) DEFAULT NULL,
  `wr_fld_per_share` varchar(300) DEFAULT NULL,
  `slongname` varchar(200) DEFAULT NULL,
  `created_at` datetime DEFAULT NULL,
  `modified_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY `fld_exchangename` (`fld_exchangename`,`fld_name`,`fld_fyear`,`oi_fld_no_of_share`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `share_holder_meeting`
--

DROP TABLE IF EXISTS `share_holder_meeting`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `share_holder_meeting` (
  `dt_tm` varchar(100) DEFAULT NULL,
  `industry_name` varchar(200) DEFAULT NULL,
  `long_name` varchar(200) DEFAULT NULL,
  `meeting_date` varchar(150) DEFAULT NULL,
  `purpose_name` varchar(250) DEFAULT NULL,
  `short_name` varchar(200) DEFAULT NULL,
  `url` varchar(300) DEFAULT NULL,
  `scrip_code` varchar(25) NOT NULL,
  `created_at` datetime DEFAULT NULL,
  `modified_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY `dt_tm` (`dt_tm`,`industry_name`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `shareholding_pattern`
--

DROP TABLE IF EXISTS `shareholding_pattern`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `shareholding_pattern` (
  `scrip_code` varchar(25) NOT NULL,
  `json_value` text,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`scrip_code`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `slb`
--

DROP TABLE IF EXISTS `slb`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `slb` (
  `bst_s_rate` varchar(200) DEFAULT NULL,
  `bst_b_rate` varchar(200) DEFAULT NULL,
  `dt_tm` varchar(50) DEFAULT NULL,
  `highrate` varchar(300) DEFAULT NULL,
  `lowrate` varchar(25) DEFAULT NULL,
  `ltradert` varchar(125) DEFAULT NULL,
  `nooftrd` varchar(50) DEFAULT NULL,
  `openrate` varchar(125) DEFAULT NULL,
  `tot_b_qty` varchar(125) DEFAULT NULL,
  `tot_s_qty` varchar(125) DEFAULT NULL,
  `trd_vol` varchar(25) DEFAULT NULL,
  `closerate` varchar(200) DEFAULT NULL,
  `expirationdate` varchar(200) DEFAULT NULL,
  `scrip_Cd` varchar(300) DEFAULT NULL,
  `scripname` varchar(300) DEFAULT NULL,
  `scrip_code` varchar(25) DEFAULT NULL,
  `created_at` datetime DEFAULT NULL,
  `modified_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY `scrip_Cd` (`scrip_Cd`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `voting`
--

DROP TABLE IF EXISTS `voting`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `voting` (
  `description` text,
  `fld_agendadetails` varchar(200) DEFAULT NULL,
  `fld_agendainterest` varchar(50) DEFAULT NULL,
  `fld_masterid` varchar(300) DEFAULT NULL,
  `fld_meetingdate` varchar(25) DEFAULT NULL,
  `fld_resoltypeid` varchar(125) DEFAULT NULL,
  `fld_scripcode` varchar(50) DEFAULT NULL,
  `fld_xmlname` varchar(125) DEFAULT NULL,
  `scripname` varchar(125) DEFAULT NULL,
  `fld_srno` varchar(125) DEFAULT NULL,
  `scrip_code` varchar(25) NOT NULL,
  `created_at` datetime DEFAULT NULL,
  `modified_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY `fld_agendadetails` (`fld_agendadetails`,`fld_meetingdate`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2020-06-16 13:12:34
