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
  `CLIENT_NAME` varchar(100) DEFAULT NULL,
  `DEAL_DATE` varchar(50) DEFAULT NULL,
  `PRICE` varchar(25) DEFAULT NULL,
  `QUANTITY` varchar(25) DEFAULT NULL,
  `SCRIP_CODE` varchar(25) DEFAULT NULL,
  `TRANSACTION_TYPE` varchar(10) DEFAULT NULL,
  `scripname` varchar(200) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `board_meeting`
--

DROP TABLE IF EXISTS `board_meeting`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `board_meeting` (
  `LONG_NAME` varchar(200) DEFAULT NULL,
  `Purpose_name` varchar(200) DEFAULT NULL,
  `Short_name` varchar(200) DEFAULT NULL,
  `meeting_date` varchar(50) DEFAULT NULL,
  `scrip_code` varchar(25) DEFAULT NULL,
  `tm` varchar(50) DEFAULT NULL
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
  `crawl_status` int(11) NOT NULL DEFAULT '0',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `modified_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`security_code`,`crawl_status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `bulk_deals`
--

DROP TABLE IF EXISTS `bulk_deals`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `bulk_deals` (
  `CLIENT_NAME` varchar(100) DEFAULT NULL,
  `DEAL_DATE` varchar(50) DEFAULT NULL,
  `PRICE` varchar(25) DEFAULT NULL,
  `QUANTITY` varchar(25) DEFAULT NULL,
  `SCRIP_CODE` varchar(25) DEFAULT NULL,
  `TRANSACTION_TYPE` varchar(10) DEFAULT NULL,
  `scripname` varchar(200) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `consolidated_pledge`
--

DROP TABLE IF EXISTS `consolidated_pledge`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `consolidated_pledge` (
  `CompanyName` varchar(100) DEFAULT NULL,
  `FLAg_Pledge` varchar(50) DEFAULT NULL,
  `F_NewCol` varchar(25) DEFAULT NULL,
  `Fld_EndDate` varchar(25) DEFAULT NULL,
  `Fld_QuarterId` varchar(25) DEFAULT NULL,
  `NoofShares_TOTAL_PROMOTER_HOLDING` varchar(10) DEFAULT NULL,
  `Noofsharespledged` varchar(200) DEFAULT NULL,
  `PROMOTEREncum_NoOfshares` varchar(100) DEFAULT NULL,
  `PROMOTEREncum_Percof_PromoterShares` varchar(100) DEFAULT NULL,
  `PROMOTEREncum_Percof_TotalShares` varchar(100) DEFAULT NULL,
  `Percentage_TOTAL_PROMOTER_HOLDING` varchar(100) DEFAULT NULL,
  `Public_NoofShares_HOLDING` varchar(100) DEFAULT NULL,
  `SHP_PulishedTime` varchar(25) DEFAULT NULL,
  `SRNO` varchar(50) DEFAULT NULL,
  `ScripCode` varchar(25) DEFAULT NULL,
  `TOTAL_NO_OF_ISSUED_SHARES` varchar(50) DEFAULT NULL,
  `TotalNoofShares` varchar(50) DEFAULT NULL,
  `dDateEnd` varchar(25) DEFAULT NULL,
  `sQtrName` varchar(100) DEFAULT NULL,
  `scrip_code` varchar(25) DEFAULT NULL,
  UNIQUE KEY `scrip_code` (`scrip_code`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `corp_action`
--

DROP TABLE IF EXISTS `corp_action`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `corp_action` (
  `Amount` varchar(20) DEFAULT NULL,
  `BCRD_from` varchar(200) DEFAULT NULL,
  `purpose_name` varchar(200) DEFAULT NULL,
  `scrip_code` varchar(25) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `corp_annexure_1`
--

DROP TABLE IF EXISTS `corp_annexure_1`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `corp_annexure_1` (
  `Fld_AffirmationLblId` varchar(200) DEFAULT NULL,
  `Fld_AffirmationLblName` varchar(200) DEFAULT NULL,
  `Fld_AppoinmentDate` varchar(50) DEFAULT NULL,
  `Fld_AuthoriseDate` varchar(50) DEFAULT NULL,
  `Fld_BODCompId` varchar(125) DEFAULT NULL,
  `Fld_BODMeetingId` varchar(125) DEFAULT NULL,
  `Fld_Category1` varchar(150) DEFAULT NULL,
  `Fld_Category2` varchar(125) DEFAULT NULL,
  `Fld_CategoryFinal` varchar(25) DEFAULT NULL,
  `Fld_CessationDate` varchar(25) DEFAULT NULL,
  `Fld_CommitteeCompId` varchar(50) DEFAULT NULL,
  `Fld_CompDetails` varchar(300) DEFAULT NULL,
  `Fld_CompStatus` varchar(200) DEFAULT NULL,
  `Fld_DIN` varchar(200) DEFAULT NULL,
  `Fld_DINReason` varchar(300) DEFAULT NULL,
  `Fld_DOB` varchar(30) DEFAULT NULL,
  `Fld_IndependantDirNum` varchar(300) DEFAULT NULL,
  `Fld_IsRequirQuorumMet` varchar(200) DEFAULT NULL,
  `Fld_MOBNotes` varchar(300) DEFAULT NULL,
  `Fld_MasterID` varchar(50) DEFAULT NULL,
  `Fld_MaxGap` varchar(300) DEFAULT NULL,
  `Fld_MeetingDatePQ` varchar(50) DEFAULT NULL,
  `Fld_MeetingDateRQ` varchar(500) DEFAULT NULL,
  `Fld_MemberName` varchar(100) DEFAULT NULL,
  `Fld_NameOfOtherCommittee` varchar(250) DEFAULT NULL,
  `Fld_NameofCommittee` varchar(150) DEFAULT NULL,
  `Fld_NameofDirectors` varchar(200) DEFAULT NULL,
  `Fld_NoOfChairperson` varchar(200) DEFAULT NULL,
  `Fld_NoOfDirectorship` varchar(100) DEFAULT NULL,
  `Fld_NoOfIndDirectorship` varchar(300) DEFAULT NULL,
  `Fld_NoOfMembership` varchar(200) DEFAULT NULL,
  `Fld_Notes` varchar(200) DEFAULT NULL,
  `Fld_PAN` varchar(50) DEFAULT NULL,
  `Fld_PresentDirNum` varchar(100) DEFAULT NULL,
  `Fld_QuarterID` varchar(50) DEFAULT NULL,
  `Fld_ReAppoinmentDate` varchar(50) DEFAULT NULL,
  `Fld_RegulationNo` varchar(50) DEFAULT NULL,
  `Fld_Salutation` varchar(100) DEFAULT NULL,
  `Fld_Scripcode` varchar(100) DEFAULT NULL,
  `Fld_Sequence` varchar(100) DEFAULT NULL,
  `Fld_Tenure` varchar(100) DEFAULT NULL,
  `scrip_code` varchar(25) DEFAULT NULL,
  UNIQUE KEY `Fld_BODCompId` (`Fld_BODCompId`),
  UNIQUE KEY `Fld_QuarterID` (`Fld_QuarterID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `corp_annexure_2`
--

DROP TABLE IF EXISTS `corp_annexure_2`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `corp_annexure_2` (
  `Fld_AffirmationLblId` varchar(200) DEFAULT NULL,
  `Fld_AffirmationLblName` varchar(200) DEFAULT NULL,
  `Fld_AppoinmentDate` varchar(50) DEFAULT NULL,
  `Fld_AuthoriseDate` varchar(50) DEFAULT NULL,
  `Fld_BODCompId` varchar(125) DEFAULT NULL,
  `Fld_BODMeetingId` varchar(125) DEFAULT NULL,
  `Fld_Category1` varchar(150) DEFAULT NULL,
  `Fld_Category2` varchar(125) DEFAULT NULL,
  `Fld_CategoryFinal` varchar(25) DEFAULT NULL,
  `Fld_CessationDate` varchar(25) DEFAULT NULL,
  `Fld_CommitteeCompId` varchar(50) DEFAULT NULL,
  `Fld_CompDetails` varchar(300) DEFAULT NULL,
  `Fld_CompStatus` varchar(200) DEFAULT NULL,
  `Fld_DIN` varchar(200) DEFAULT NULL,
  `Fld_DINReason` varchar(300) DEFAULT NULL,
  `Fld_DOB` varchar(30) DEFAULT NULL,
  `Fld_IndependantDirNum` varchar(300) DEFAULT NULL,
  `Fld_IsRequirQuorumMet` varchar(200) DEFAULT NULL,
  `Fld_MOBNotes` varchar(300) DEFAULT NULL,
  `Fld_MasterID` varchar(50) DEFAULT NULL,
  `Fld_MaxGap` varchar(300) DEFAULT NULL,
  `Fld_MeetingDatePQ` varchar(50) DEFAULT NULL,
  `Fld_MeetingDateRQ` varchar(500) DEFAULT NULL,
  `Fld_MemberName` varchar(100) DEFAULT NULL,
  `Fld_NameOfOtherCommittee` varchar(250) DEFAULT NULL,
  `Fld_NameofCommittee` varchar(150) DEFAULT NULL,
  `Fld_NameofDirectors` varchar(200) DEFAULT NULL,
  `Fld_NoOfChairperson` varchar(200) DEFAULT NULL,
  `Fld_NoOfDirectorship` varchar(100) DEFAULT NULL,
  `Fld_NoOfIndDirectorship` varchar(300) DEFAULT NULL,
  `Fld_NoOfMembership` varchar(200) DEFAULT NULL,
  `Fld_Notes` varchar(200) DEFAULT NULL,
  `Fld_PAN` varchar(50) DEFAULT NULL,
  `Fld_PresentDirNum` varchar(100) DEFAULT NULL,
  `Fld_QuarterID` varchar(50) DEFAULT NULL,
  `Fld_ReAppoinmentDate` varchar(50) DEFAULT NULL,
  `Fld_RegulationNo` varchar(50) DEFAULT NULL,
  `Fld_Salutation` varchar(100) DEFAULT NULL,
  `Fld_Scripcode` varchar(100) DEFAULT NULL,
  `Fld_Sequence` varchar(100) DEFAULT NULL,
  `Fld_Tenure` varchar(100) DEFAULT NULL,
  UNIQUE KEY `Fld_BODCompId` (`Fld_BODCompId`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `corp_announcement`
--

DROP TABLE IF EXISTS `corp_announcement`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `corp_announcement` (
  `AGENDA_ID` varchar(200) DEFAULT NULL,
  `ANNOUNCEMENT_TYPE` varchar(200) DEFAULT NULL,
  `ATTACHMENTNAME` varchar(250) DEFAULT NULL,
  `CATEGORYNAME` varchar(300) DEFAULT NULL,
  `CRITICALNEWS` varchar(25) DEFAULT NULL,
  `DT_TM` varchar(125) DEFAULT NULL,
  `DissemDT` varchar(50) DEFAULT NULL,
  `FILESTATUS` varchar(125) DEFAULT NULL,
  `HEADLINE` varchar(125) DEFAULT NULL,
  `MORE` varchar(125) DEFAULT NULL,
  `NEWSID` varchar(25) DEFAULT NULL,
  `NEWSSUB` varchar(200) DEFAULT NULL,
  `NEWS_DT` varchar(200) DEFAULT NULL,
  `NSURL` varchar(300) DEFAULT NULL,
  `News_submission_dt` varchar(300) DEFAULT NULL,
  `OLD` varchar(300) DEFAULT NULL,
  `PDFFLAG` varchar(300) DEFAULT NULL,
  `QUARTER_ID` varchar(200) DEFAULT NULL,
  `RN` varchar(300) DEFAULT NULL,
  `SCRIP_CD` varchar(200) DEFAULT NULL,
  `SLONGNAME` varchar(200) DEFAULT NULL,
  `TimeDiff` varchar(50) DEFAULT NULL,
  `TotalPageCnt` varchar(100) DEFAULT NULL,
  `XML_NAME` varchar(300) DEFAULT NULL,
  UNIQUE KEY `NEWSID` (`NEWSID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `corp_info`
--

DROP TABLE IF EXISTS `corp_info`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `corp_info` (
  `sDesignation` varchar(200) DEFAULT NULL,
  `sFirstname` varchar(200) DEFAULT NULL,
  `sLastname` varchar(200) DEFAULT NULL,
  `sMiddlename` varchar(200) DEFAULT NULL,
  `sPrefix` varchar(25) DEFAULT NULL,
  `scrip_code` varchar(25) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `debt`
--

DROP TABLE IF EXISTS `debt`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `debt` (
  `Change_` varchar(100) DEFAULT NULL,
  `Dt_tm` varchar(50) DEFAULT NULL,
  `HighRate` varchar(25) DEFAULT NULL,
  `ISIN_NUMBER` varchar(50) DEFAULT NULL,
  `Instrument` varchar(125) DEFAULT NULL,
  `LowRate` varchar(100) DEFAULT NULL,
  `Ltradert` varchar(200) DEFAULT NULL,
  `OpenRate` varchar(100) DEFAULT NULL,
  `ScripName` varchar(100) DEFAULT NULL,
  `TURNOVER` varchar(100) DEFAULT NULL,
  `VOLUME` varchar(100) DEFAULT NULL,
  `YTM` varchar(100) DEFAULT NULL,
  `change_percent` varchar(125) DEFAULT NULL,
  `scrip_cd` varchar(50) DEFAULT NULL,
  `scrip_code` varchar(25) DEFAULT NULL,
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
  `BroadcastDTtm` varchar(100) DEFAULT NULL,
  `Buy_Sell` varchar(50) DEFAULT NULL,
  `Company_Name` varchar(200) DEFAULT NULL,
  `FLD_MODE` varchar(150) DEFAULT NULL,
  `Flag` varchar(50) DEFAULT NULL,
  `Fld_AcqSoldDateFrom` varchar(100) DEFAULT NULL,
  `Fld_AcqSoldDateTo` varchar(200) DEFAULT NULL,
  `Insider_name` varchar(100) DEFAULT NULL,
  `Modified_date` varchar(100) DEFAULT NULL,
  `Perc_After` varchar(100) DEFAULT NULL,
  `Perc_of_Buysell` varchar(100) DEFAULT NULL,
  `Quantity` varchar(100) DEFAULT NULL,
  `Quantity_after` varchar(125) DEFAULT NULL,
  `scrip_code` varchar(25) DEFAULT NULL,
  `transaction_date` varchar(50) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `insider_2015`
--

DROP TABLE IF EXISTS `insider_2015`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `insider_2015` (
  `Companyname` varchar(200) DEFAULT NULL,
  `Fld_AttachmentICRA` varchar(200) DEFAULT NULL,
  `Fld_AuthoriseDate` varchar(50) DEFAULT NULL,
  `Fld_ContractSpecifications` varchar(300) DEFAULT NULL,
  `Fld_CreateDate` varchar(25) DEFAULT NULL,
  `Fld_DateIntimation` varchar(25) DEFAULT NULL,
  `Fld_ExportFlag` varchar(50) DEFAULT NULL,
  `Fld_FromDate` varchar(25) DEFAULT NULL,
  `Fld_FromDateAllotment` varchar(25) DEFAULT NULL,
  `Fld_ID` varchar(25) DEFAULT NULL,
  `Fld_LetterDate` varchar(25) DEFAULT NULL,
  `Fld_Mode` varchar(200) DEFAULT NULL,
  `Fld_ModeofAcquisition` varchar(200) DEFAULT NULL,
  `Fld_Notes` text,
  `Fld_PercentofShareholdingPost` varchar(300) DEFAULT NULL,
  `Fld_PercentofShareholdingPre` varchar(300) DEFAULT NULL,
  `Fld_PersonCatgName` varchar(300) DEFAULT NULL,
  `Fld_PromoterAdd` varchar(200) DEFAULT NULL,
  `Fld_PromoterCIN` varchar(30) DEFAULT NULL,
  `Fld_PromoterCatg` varchar(200) DEFAULT NULL,
  `Fld_PromoterContact` varchar(300) DEFAULT NULL,
  `Fld_PromoterDIN` varchar(100) DEFAULT NULL,
  `Fld_PromoterName` varchar(500) DEFAULT NULL,
  `Fld_PromoterPAN` varchar(100) DEFAULT NULL,
  `Fld_ScripCode` varchar(50) DEFAULT NULL,
  `Fld_SecurityNo` varchar(50) DEFAULT NULL,
  `Fld_SecurityNoPost` varchar(200) DEFAULT NULL,
  `Fld_SecurityNoPrior` varchar(200) DEFAULT NULL,
  `Fld_SecurityType` varchar(100) DEFAULT NULL,
  `Fld_SecurityTypeName` varchar(300) DEFAULT NULL,
  `Fld_SecurityTypeNamePrior` varchar(200) DEFAULT NULL,
  `Fld_SecurityTypePost` varchar(200) DEFAULT NULL,
  `Fld_SecurityTypePrior` varchar(200) DEFAULT NULL,
  `Fld_SecurityValue` varchar(200) DEFAULT NULL,
  `Fld_StampDate` varchar(50) DEFAULT NULL,
  `Fld_ToDate` varchar(50) DEFAULT NULL,
  `Fld_ToDateAllotment` varchar(50) DEFAULT NULL,
  `Fld_TradeDerivBuyUnits` varchar(100) DEFAULT NULL,
  `Fld_TradeDerivBuyValue` varchar(100) DEFAULT NULL,
  `Fld_TradeDerivSellUnits` varchar(100) DEFAULT NULL,
  `Fld_TradeDerivSellValue` varchar(100) DEFAULT NULL,
  `Fld_TradeExchange` varchar(100) DEFAULT NULL,
  `Fld_TransactionType` varchar(100) DEFAULT NULL,
  `Fld_TypeofContract` varchar(100) DEFAULT NULL,
  `Fld_UpdateDate` varchar(50) DEFAULT NULL,
  `Fld_attachment` varchar(300) DEFAULT NULL,
  `ModeOfAquisation` varchar(300) DEFAULT NULL,
  `scrip_code` varchar(25) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `insider_sast`
--

DROP TABLE IF EXISTS `insider_sast`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `insider_sast` (
  `Acq_Sale` varchar(100) DEFAULT NULL,
  `Acq_sale_Pct` varchar(100) DEFAULT NULL,
  `Acq_sale_qty` varchar(100) DEFAULT NULL,
  `Acquisition_After` varchar(150) DEFAULT NULL,
  `Acquisition_Pct_After` varchar(50) DEFAULT NULL,
  `Acquisition_date` varchar(100) DEFAULT NULL,
  `Company_Name` varchar(200) DEFAULT NULL,
  `FLD_MODE` varchar(100) DEFAULT NULL,
  `Fld_AcqSoldDateFrom` varchar(100) DEFAULT NULL,
  `Fld_NameOfPG` varchar(100) DEFAULT NULL,
  `Fld_TotDilPerAfterAcq` varchar(100) DEFAULT NULL,
  `NEWDT` varchar(100) DEFAULT NULL,
  `flag` varchar(125) DEFAULT NULL,
  `modify_date` varchar(50) DEFAULT NULL,
  `ord` varchar(100) DEFAULT NULL,
  `scrip_code` varchar(25) DEFAULT NULL,
  `shareholdername` varchar(200) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `notice`
--

DROP TABLE IF EXISTS `notice`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `notice` (
  `notice_no` varchar(100) DEFAULT NULL,
  `subject` varchar(200) DEFAULT NULL,
  `subject_link` varchar(250) DEFAULT NULL,
  `segmentname` varchar(125) DEFAULT NULL,
  `categoryname` varchar(125) DEFAULT NULL,
  `departmentname` varchar(125) DEFAULT NULL,
  `scrip_code` varchar(25) DEFAULT NULL,
  UNIQUE KEY `notice_no` (`notice_no`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `peer`
--

DROP TABLE IF EXISTS `peer`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `peer` (
  `BV` varchar(200) DEFAULT NULL,
  `Bodies_Corporate` varchar(200) DEFAULT NULL,
  `Cash_EPS` varchar(150) DEFAULT NULL,
  `Change_` varchar(300) DEFAULT NULL,
  `DII` varchar(125) DEFAULT NULL,
  `EPS` varchar(125) DEFAULT NULL,
  `Equity` varchar(150) DEFAULT NULL,
  `FACE_VALUE` varchar(125) DEFAULT NULL,
  `FII` varchar(125) DEFAULT NULL,
  `Foreign_` varchar(125) DEFAULT NULL,
  `INDIAN` varchar(25) DEFAULT NULL,
  `Institution` varchar(200) DEFAULT NULL,
  `LTP` varchar(200) DEFAULT NULL,
  `NPM` varchar(300) DEFAULT NULL,
  `Name` varchar(300) DEFAULT NULL,
  `NonIns` varchar(300) DEFAULT NULL,
  `OPM` varchar(300) DEFAULT NULL,
  `PAT` varchar(200) DEFAULT NULL,
  `PB` varchar(30) DEFAULT NULL,
  `PE` varchar(200) DEFAULT NULL,
  `PnPGrp` varchar(300) DEFAULT NULL,
  `Public` varchar(100) DEFAULT NULL,
  `RONW` varchar(500) DEFAULT NULL,
  `ResQtr` varchar(100) DEFAULT NULL,
  `Results_QuarterName` varchar(150) DEFAULT NULL,
  `Revenue` varchar(50) DEFAULT NULL,
  `SHP_QuarterName` varchar(200) DEFAULT NULL,
  `scrip_cd` varchar(200) DEFAULT NULL,
  `w52hi` varchar(100) DEFAULT NULL,
  `w52hidt` varchar(100) DEFAULT NULL,
  `w52lo` varchar(100) DEFAULT NULL,
  `w52lodt` varchar(200) DEFAULT NULL,
  `scrip_code` varchar(25) DEFAULT NULL,
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
  `2020` varchar(100) DEFAULT NULL,
  `2019` varchar(100) DEFAULT NULL,
  `2018` varchar(100) DEFAULT NULL,
  `2017` varchar(100) DEFAULT NULL,
  `2016` varchar(100) DEFAULT NULL,
  `Income Statement` varchar(200) DEFAULT NULL,
  `scrip_code` varchar(25) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `results_qrt`
--

DROP TABLE IF EXISTS `results_qrt`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `results_qrt` (
  `Mar-20` varchar(200) DEFAULT NULL,
  `Dec-19` varchar(200) DEFAULT NULL,
  `Sep-19` varchar(200) DEFAULT NULL,
  `Jun-19` varchar(200) DEFAULT NULL,
  `Mar-19` varchar(200) DEFAULT NULL,
  `FY 19-20` varchar(200) DEFAULT NULL,
  `Income Statement` varchar(200) DEFAULT NULL,
  `scrip_code` varchar(25) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `sast_annual`
--

DROP TABLE IF EXISTS `sast_annual`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `sast_annual` (
  `CS_Fld_No_of_Share` varchar(200) DEFAULT NULL,
  `CS_Fld_Per_DilutedShare` varchar(200) DEFAULT NULL,
  `CS_Fld_Per_Share` varchar(150) DEFAULT NULL,
  `Fld_ExchangeName` varchar(300) DEFAULT NULL,
  `Fld_FYear` varchar(25) DEFAULT NULL,
  `Fld_Name` varchar(125) DEFAULT NULL,
  `Fld_ScripCode` varchar(50) DEFAULT NULL,
  `OI_Fld_No_of_Share` varchar(125) DEFAULT NULL,
  `OI_Fld_Per_DilutedShare` varchar(125) DEFAULT NULL,
  `OI_Fld_Per_Share` varchar(125) DEFAULT NULL,
  `SH_Fld_No_of_Share` varchar(25) DEFAULT NULL,
  `SH_Fld_Per_DilutedShare` varchar(200) DEFAULT NULL,
  `SH_Fld_Per_Share` varchar(200) DEFAULT NULL,
  `VR_Fld_No_of_Share` varchar(300) DEFAULT NULL,
  `VR_Fld_Per_DilutedShare` varchar(300) DEFAULT NULL,
  `VR_Fld_Per_Share` varchar(300) DEFAULT NULL,
  `WR_Fld_No_of_Share` varchar(300) DEFAULT NULL,
  `WR_Fld_Per_DilutedShare` varchar(200) DEFAULT NULL,
  `WR_Fld_Per_Share` varchar(300) DEFAULT NULL,
  `sLongName` varchar(200) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `share_holder_meeting`
--

DROP TABLE IF EXISTS `share_holder_meeting`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `share_holder_meeting` (
  `DT_TM` varchar(100) DEFAULT NULL,
  `Industry_name` varchar(200) DEFAULT NULL,
  `Long_Name` varchar(200) DEFAULT NULL,
  `MEETING_DATE` varchar(150) DEFAULT NULL,
  `PURPOSE_NAME` varchar(250) DEFAULT NULL,
  `Short_name` varchar(200) DEFAULT NULL,
  `URL` varchar(300) DEFAULT NULL,
  `scrip_code` varchar(25) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `shareholding_pattern`
--

DROP TABLE IF EXISTS `shareholding_pattern`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `shareholding_pattern` (
  `Category of shareholder` varchar(200) DEFAULT NULL,
  `(A) Promoter & Promoter Group` varchar(200) DEFAULT NULL,
  `(B) Public` varchar(200) DEFAULT NULL,
  `(C1) Shares underlying DRs` varchar(200) DEFAULT NULL,
  `(C2) Shares held by Employee Trust` varchar(200) DEFAULT NULL,
  `(C) Non Promoter-Non Public` varchar(200) DEFAULT NULL,
  `Grand Total` varchar(200) DEFAULT NULL,
  `scrip_code` varchar(25) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `slb`
--

DROP TABLE IF EXISTS `slb`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `slb` (
  `Bst_S_rate` varchar(200) DEFAULT NULL,
  `Bst_b_rate` varchar(200) DEFAULT NULL,
  `Dt_tm` varchar(50) DEFAULT NULL,
  `HighRate` varchar(300) DEFAULT NULL,
  `LowRate` varchar(25) DEFAULT NULL,
  `Ltradert` varchar(125) DEFAULT NULL,
  `NoOfTrd` varchar(50) DEFAULT NULL,
  `OpenRate` varchar(125) DEFAULT NULL,
  `Tot_B_qty` varchar(125) DEFAULT NULL,
  `Tot_S_qty` varchar(125) DEFAULT NULL,
  `Trd_vol` varchar(25) DEFAULT NULL,
  `closerate` varchar(200) DEFAULT NULL,
  `expirationdate` varchar(200) DEFAULT NULL,
  `scrip_Cd` varchar(300) DEFAULT NULL,
  `scripname` varchar(300) DEFAULT NULL,
  `scrip_code` varchar(25) DEFAULT NULL,
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
  `Description` text,
  `Fld_AgendaDetails` varchar(200) DEFAULT NULL,
  `Fld_AgendaInterest` varchar(50) DEFAULT NULL,
  `Fld_MasterID` varchar(300) DEFAULT NULL,
  `Fld_MeetingDate` varchar(25) DEFAULT NULL,
  `Fld_ResolTypeID` varchar(125) DEFAULT NULL,
  `Fld_Scripcode` varchar(50) DEFAULT NULL,
  `Fld_XMLName` varchar(125) DEFAULT NULL,
  `ScripName` varchar(125) DEFAULT NULL,
  `fld_srno` varchar(125) DEFAULT NULL,
  `scrip_code` varchar(25) DEFAULT NULL
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

-- Dump completed on 2020-06-10 16:46:39
