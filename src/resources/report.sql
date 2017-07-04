/*
Navicat MySQL Data Transfer

Source Server         : hadoop_3306
Source Server Version : 50718
Source Host           : hadoop:3306
Source Database       : report

Target Server Type    : MYSQL
Target Server Version : 50718
File Encoding         : 65001

Date: 2017-07-03 18:39:19
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for dimension_browser
-- ----------------------------
DROP TABLE IF EXISTS `dimension_browser`;
CREATE TABLE `dimension_browser` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `browser_name` varchar(45) NOT NULL DEFAULT '' COMMENT '浏览器名称',
  `browser_version` varchar(255) NOT NULL DEFAULT '' COMMENT '浏览器版本号',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='浏览器维度信息表';

-- ----------------------------
-- Records of dimension_browser
-- ----------------------------

-- ----------------------------
-- Table structure for dimension_currency_type
-- ----------------------------
DROP TABLE IF EXISTS `dimension_currency_type`;
CREATE TABLE `dimension_currency_type` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `currency_name` varchar(10) DEFAULT '' COMMENT '货币名称',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='支付货币类型维度信息表';

-- ----------------------------
-- Records of dimension_currency_type
-- ----------------------------

-- ----------------------------
-- Table structure for dimension_date
-- ----------------------------
DROP TABLE IF EXISTS `dimension_date`;
CREATE TABLE `dimension_date` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '日期id',
  `year` int(11) DEFAULT NULL COMMENT '年',
  `season` int(11) DEFAULT NULL COMMENT '季度',
  `month` int(11) DEFAULT NULL COMMENT '月份',
  `week` int(11) DEFAULT NULL COMMENT '周',
  `day` int(11) DEFAULT NULL COMMENT '天',
  `calendar` date DEFAULT NULL COMMENT '对应日期',
  `type` enum('year','season','month','week','') DEFAULT NULL COMMENT '日期格式',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='时间维度信息表';

-- ----------------------------
-- Records of dimension_date
-- ----------------------------

-- ----------------------------
-- Table structure for dimension_event
-- ----------------------------
DROP TABLE IF EXISTS `dimension_event`;
CREATE TABLE `dimension_event` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '时间id',
  `category` varchar(255) DEFAULT NULL COMMENT '事件种类',
  `action` varchar(255) DEFAULT NULL COMMENT '事件动作',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='事件维度信息表';

-- ----------------------------
-- Records of dimension_event
-- ----------------------------

-- ----------------------------
-- Table structure for dimension_inbound
-- ----------------------------
DROP TABLE IF EXISTS `dimension_inbound`;
CREATE TABLE `dimension_inbound` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '外链id',
  `parent_id` int(11) DEFAULT NULL COMMENT '父级外链id',
  `name` varchar(45) DEFAULT NULL COMMENT '外链名称',
  `url` varchar(255) DEFAULT NULL COMMENT '外链url',
  `type` int(11) DEFAULT NULL COMMENT '外链类型',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='外链源数据维度信息表';

-- ----------------------------
-- Records of dimension_inbound
-- ----------------------------

-- ----------------------------
-- Table structure for dimension_kpi
-- ----------------------------
DROP TABLE IF EXISTS `dimension_kpi`;
CREATE TABLE `dimension_kpi` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT 'KPI id',
  `kpi_name` varchar(45) DEFAULT NULL COMMENT 'KPI维度名称',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='KPI维度相关信息表';

-- ----------------------------
-- Records of dimension_kpi
-- ----------------------------

-- ----------------------------
-- Table structure for dimension_location
-- ----------------------------
DROP TABLE IF EXISTS `dimension_location`;
CREATE TABLE `dimension_location` (
  `id` int(11) NOT NULL COMMENT '地域id',
  `country` varchar(45) DEFAULT NULL COMMENT '国家名称',
  `province` varchar(45) DEFAULT NULL COMMENT '省份名称',
  `city` varchar(45) DEFAULT NULL COMMENT '城市名称',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='地域信息维度表';

-- ----------------------------
-- Records of dimension_location
-- ----------------------------

-- ----------------------------
-- Table structure for dimension_os
-- ----------------------------
DROP TABLE IF EXISTS `dimension_os`;
CREATE TABLE `dimension_os` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '操作系统id',
  `os_name` varchar(45) DEFAULT NULL COMMENT '操作系统名称',
  `os_version` varchar(45) DEFAULT NULL COMMENT '操作系统版本号',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='操作系统信息维度表';

-- ----------------------------
-- Records of dimension_os
-- ----------------------------

-- ----------------------------
-- Table structure for dimension_payment_type
-- ----------------------------
DROP TABLE IF EXISTS `dimension_payment_type`;
CREATE TABLE `dimension_payment_type` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '支付方式id',
  `payment_type` varchar(255) DEFAULT NULL COMMENT '支付方式名称',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='支付方式维度信息表';

-- ----------------------------
-- Records of dimension_payment_type
-- ----------------------------

-- ----------------------------
-- Table structure for dimension_platform
-- ----------------------------
DROP TABLE IF EXISTS `dimension_platform`;
CREATE TABLE `dimension_platform` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '平台id',
  `platform_name` varchar(45) DEFAULT NULL COMMENT '平台名称',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='平台维度信息表';

-- ----------------------------
-- Records of dimension_platform
-- ----------------------------

-- ----------------------------
-- Table structure for event_info
-- ----------------------------
DROP TABLE IF EXISTS `event_info`;
CREATE TABLE `event_info` (
  `event_dimension_id` int(11) NOT NULL COMMENT '事件id',
  `key` varchar(255) DEFAULT NULL COMMENT '事件中的变量名',
  `value` varchar(255) DEFAULT NULL COMMENT '事件中变量值',
  `times` int(11) DEFAULT '0' COMMENT '触发次数',
  PRIMARY KEY (`event_dimension_id`),
  CONSTRAINT `FK_event_event_info` FOREIGN KEY (`event_dimension_id`) REFERENCES `dimension_event` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='描述event的属性信息，在本次项目中不会用到';

-- ----------------------------
-- Records of event_info
-- ----------------------------

-- ----------------------------
-- Table structure for order_info
-- ----------------------------
DROP TABLE IF EXISTS `order_info`;
CREATE TABLE `order_info` (
  `order_id` varchar(50) NOT NULL DEFAULT '' COMMENT '订单id',
  `date_dimension_id` int(11) DEFAULT '0' COMMENT '订单日期id',
  `amount` int(11) DEFAULT '0' COMMENT '订单金额',
  `is_pay` int(1) DEFAULT '0' COMMENT '是否支付，0表示未支付，1表示已支付',
  `is_refund` int(1) DEFAULT '0' COMMENT '是否退款，0表示未退款，1表示已退款',
  PRIMARY KEY (`order_id`),
  KEY `FK_date_order_info` (`date_dimension_id`),
  CONSTRAINT `FK_date_order_info` FOREIGN KEY (`date_dimension_id`) REFERENCES `dimension_date` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='描述订单的相关信息，该table在本次项目中的主要目标就是为了去重数据';

-- ----------------------------
-- Records of order_info
-- ----------------------------

-- ----------------------------
-- Table structure for stats_device_browser
-- ----------------------------
DROP TABLE IF EXISTS `stats_device_browser`;
CREATE TABLE `stats_device_browser` (
  `platform_dimension_id` int(11) NOT NULL DEFAULT '0' COMMENT '平台id',
  `date_dimension_id` int(11) NOT NULL DEFAULT '0' COMMENT '日期id',
  `browser_dimension_id` int(11) NOT NULL DEFAULT '0' COMMENT '浏览器id',
  `active_users` int(11) DEFAULT '0' COMMENT '活跃用户数',
  `new_install_users` int(11) DEFAULT '0' COMMENT '新增用户数',
  `total_install_users` int(11) DEFAULT '0' COMMENT '总用户数',
  `sessions` int(11) DEFAULT '0' COMMENT '会话个数',
  `sessions_length` int(11) DEFAULT '0' COMMENT '会话长度',
  `active_members` int(11) DEFAULT '0' COMMENT '活跃会员数',
  `new_members` int(11) DEFAULT '0' COMMENT '新增会员数',
  `total_members` int(11) DEFAULT '0' COMMENT '总会员数',
  `pv` int(11) DEFAULT '0' COMMENT 'pv数',
  `created` date DEFAULT NULL COMMENT '最后修改日期',
  PRIMARY KEY (`platform_dimension_id`,`date_dimension_id`,`browser_dimension_id`),
  KEY `FK_date_device_browser` (`date_dimension_id`),
  KEY `FK_browser_device_browser` (`browser_dimension_id`),
  CONSTRAINT `FK_browser_device_browser` FOREIGN KEY (`browser_dimension_id`) REFERENCES `dimension_browser` (`id`),
  CONSTRAINT `FK_date_device_browser` FOREIGN KEY (`date_dimension_id`) REFERENCES `dimension_date` (`id`),
  CONSTRAINT `FK_platform_device_browser` FOREIGN KEY (`platform_dimension_id`) REFERENCES `dimension_platform` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='统计浏览器相关分析数据的统计表';

-- ----------------------------
-- Records of stats_device_browser
-- ----------------------------

-- ----------------------------
-- Table structure for stats_device_location
-- ----------------------------
DROP TABLE IF EXISTS `stats_device_location`;
CREATE TABLE `stats_device_location` (
  `platform_dimension_id` int(11) NOT NULL DEFAULT '0' COMMENT '平台id',
  `date_dimension_id` int(11) NOT NULL DEFAULT '0' COMMENT '日期id',
  `locaction_dimension_id` int(11) NOT NULL DEFAULT '0' COMMENT '地域id',
  `active_users` int(11) DEFAULT '0' COMMENT '活跃用户数',
  `sessions` int(11) DEFAULT '0' COMMENT '会话个数',
  `bounce_sessions` int(11) DEFAULT '0' COMMENT '跳出会话个数',
  `created` date DEFAULT NULL COMMENT '最后修改日期',
  PRIMARY KEY (`platform_dimension_id`,`date_dimension_id`,`locaction_dimension_id`),
  KEY `FK_date_device_location` (`date_dimension_id`),
  KEY `FK_location_device_location` (`locaction_dimension_id`),
  CONSTRAINT `FK_date_device_location` FOREIGN KEY (`date_dimension_id`) REFERENCES `dimension_date` (`id`),
  CONSTRAINT `FK_location_device_location` FOREIGN KEY (`locaction_dimension_id`) REFERENCES `dimension_location` (`id`),
  CONSTRAINT `FK_plateform_device_location` FOREIGN KEY (`platform_dimension_id`) REFERENCES `dimension_platform` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='统计地域相关分析数据的统计表';

-- ----------------------------
-- Records of stats_device_location
-- ----------------------------

-- ----------------------------
-- Table structure for stats_event
-- ----------------------------
DROP TABLE IF EXISTS `stats_event`;
CREATE TABLE `stats_event` (
  `plateform_dimension_id` int(11) NOT NULL DEFAULT '0' COMMENT '平台id',
  `date_dimension_id` int(11) NOT NULL DEFAULT '0' COMMENT '日期id',
  `event_dimension_id` int(11) NOT NULL DEFAULT '0' COMMENT 'event维度id',
  `times` int(11) DEFAULT '0' COMMENT '事件触发次数',
  `created` date DEFAULT NULL COMMENT '最后修改时间',
  PRIMARY KEY (`plateform_dimension_id`,`date_dimension_id`,`event_dimension_id`),
  KEY `FK_date_event` (`date_dimension_id`),
  KEY `FK_event_event` (`event_dimension_id`),
  CONSTRAINT `FK_date_event` FOREIGN KEY (`date_dimension_id`) REFERENCES `dimension_date` (`id`),
  CONSTRAINT `FK_event_event` FOREIGN KEY (`event_dimension_id`) REFERENCES `dimension_event` (`id`),
  CONSTRAINT `FK_platform_event` FOREIGN KEY (`plateform_dimension_id`) REFERENCES `dimension_platform` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='统计事件相关分析数据的统计表';

-- ----------------------------
-- Records of stats_event
-- ----------------------------

-- ----------------------------
-- Table structure for stats_inbound
-- ----------------------------
DROP TABLE IF EXISTS `stats_inbound`;
CREATE TABLE `stats_inbound` (
  `platform_dimension_id` int(11) NOT NULL DEFAULT '0' COMMENT '平台id',
  `date_dimension_id` int(11) NOT NULL DEFAULT '0' COMMENT '日期id',
  `inbound_dimension_id` int(11) NOT NULL DEFAULT '0' COMMENT '外链id',
  `active_users` int(11) DEFAULT '0' COMMENT '活跃用户数',
  `sessions` int(11) DEFAULT '0' COMMENT '会话个数',
  `bounce_sessions` int(11) DEFAULT '0' COMMENT '跳出会话个数',
  `created` date DEFAULT NULL COMMENT '最后修改日期',
  PRIMARY KEY (`platform_dimension_id`,`date_dimension_id`,`inbound_dimension_id`),
  KEY `FK_date_inbound` (`date_dimension_id`),
  KEY `FK_inbound_inbound` (`inbound_dimension_id`),
  CONSTRAINT `FK_date_inbound` FOREIGN KEY (`date_dimension_id`) REFERENCES `dimension_date` (`id`),
  CONSTRAINT `FK_inbound_inbound` FOREIGN KEY (`inbound_dimension_id`) REFERENCES `dimension_inbound` (`id`),
  CONSTRAINT `FK_platform_inbound` FOREIGN KEY (`platform_dimension_id`) REFERENCES `dimension_platform` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='统计外链信息的统计表';

-- ----------------------------
-- Records of stats_inbound
-- ----------------------------

-- ----------------------------
-- Table structure for stats_order
-- ----------------------------
DROP TABLE IF EXISTS `stats_order`;
CREATE TABLE `stats_order` (
  `platform_dimension_id` int(11) NOT NULL DEFAULT '0' COMMENT '平台id',
  `date_dimension_id` int(11) NOT NULL DEFAULT '0' COMMENT '日期id',
  `currency_type_dimension_id` int(11) NOT NULL DEFAULT '0' COMMENT '货币类型id',
  `payment_type_dimension_id` int(11) NOT NULL DEFAULT '0' COMMENT '支付类型',
  `orders` int(11) DEFAULT '0' COMMENT '总订单数量',
  `success_orders` int(11) DEFAULT '0' COMMENT '支付成功订单数',
  `refund_orders` int(11) DEFAULT '0' COMMENT '退款的订单数量',
  `order_amount` int(11) DEFAULT '0' COMMENT '订单金额',
  `revenue_amount` int(11) DEFAULT '0' COMMENT '支付金额',
  `refund_amount` int(11) DEFAULT '0' COMMENT '退款金额',
  `total_revenue_amount` int(11) DEFAULT '0' COMMENT '总支付金额',
  `total_refund_amount` int(11) DEFAULT '0' COMMENT '总退款金额',
  `created` date DEFAULT NULL COMMENT '最后修改时间',
  PRIMARY KEY (`platform_dimension_id`,`date_dimension_id`,`currency_type_dimension_id`,`payment_type_dimension_id`),
  KEY `FK_date_order` (`date_dimension_id`),
  KEY `FK_currency_type_order` (`currency_type_dimension_id`),
  KEY `FK_payment_type_order` (`payment_type_dimension_id`),
  CONSTRAINT `FK_currency_type_order` FOREIGN KEY (`currency_type_dimension_id`) REFERENCES `dimension_currency_type` (`id`),
  CONSTRAINT `FK_date_order` FOREIGN KEY (`date_dimension_id`) REFERENCES `dimension_date` (`id`),
  CONSTRAINT `FK_payment_type_order` FOREIGN KEY (`payment_type_dimension_id`) REFERENCES `dimension_payment_type` (`id`),
  CONSTRAINT `FK_platform_order` FOREIGN KEY (`platform_dimension_id`) REFERENCES `dimension_platform` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='统计订单信息的统计表';

-- ----------------------------
-- Records of stats_order
-- ----------------------------

-- ----------------------------
-- Table structure for stats_user
-- ----------------------------
DROP TABLE IF EXISTS `stats_user`;
CREATE TABLE `stats_user` (
  `platform_dimension_id` int(11) NOT NULL DEFAULT '0' COMMENT '平台id',
  `date_dimension_id` int(11) NOT NULL DEFAULT '0' COMMENT '日期id',
  `active_users` int(11) DEFAULT '0' COMMENT '活跃用户数',
  `new_install_users` int(11) DEFAULT '0' COMMENT '新增用户数',
  `total_install_users` int(11) DEFAULT '0' COMMENT '总用户数',
  `sessions` int(11) DEFAULT '0' COMMENT '会话个数',
  `sessions_length` int(11) DEFAULT '0' COMMENT '会话长度',
  `active_members` int(11) DEFAULT '0' COMMENT '活跃会员数',
  `new_members` int(11) DEFAULT '0' COMMENT '新增会员数',
  `total_members` int(11) DEFAULT '0' COMMENT '总会员数',
  `created` date DEFAULT NULL COMMENT '记录日期',
  PRIMARY KEY (`platform_dimension_id`,`date_dimension_id`),
  KEY `FK_date_user` (`date_dimension_id`),
  CONSTRAINT `FK_date_user` FOREIGN KEY (`date_dimension_id`) REFERENCES `dimension_date` (`id`),
  CONSTRAINT `FK_platform_user` FOREIGN KEY (`platform_dimension_id`) REFERENCES `dimension_platform` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='统计用户基本信息的统计表';

-- ----------------------------
-- Records of stats_user
-- ----------------------------

-- ----------------------------
-- Table structure for stats_view_depth
-- ----------------------------
DROP TABLE IF EXISTS `stats_view_depth`;
CREATE TABLE `stats_view_depth` (
  `platform_dimension_id` int(11) NOT NULL DEFAULT '0' COMMENT '平台id',
  `date_dimension_id` int(11) NOT NULL DEFAULT '0' COMMENT '日期id',
  `kpi_dimension_id` int(11) NOT NULL DEFAULT '0' COMMENT 'KPI id',
  `pv1` int(11) DEFAULT '0' COMMENT '只访问一个页面的数量',
  `pv2` int(11) DEFAULT '0' COMMENT '只访问两个页面的数量',
  `pv3` int(11) DEFAULT '0' COMMENT '只访问三个页面的数量',
  `pv4` int(11) DEFAULT '0' COMMENT '只访问四个页面的数量',
  `pv5_10` int(11) DEFAULT '0' COMMENT '访问[5, 10)个页面的数量',
  `pv10_30` int(11) DEFAULT '0' COMMENT '访问[10, 30)个页面的数量',
  `pv30_60` int(11) DEFAULT '0' COMMENT '访问[30, 60)个页面的数量',
  `pv60+` int(11) DEFAULT '0' COMMENT '访问[60, ...)个页面的数量',
  `created` date DEFAULT NULL COMMENT '最后修改日期',
  PRIMARY KEY (`platform_dimension_id`,`date_dimension_id`,`kpi_dimension_id`),
  KEY `FK_date_view_depth` (`date_dimension_id`),
  KEY `FK_kpi_view_depth` (`kpi_dimension_id`),
  CONSTRAINT `FK_date_view_depth` FOREIGN KEY (`date_dimension_id`) REFERENCES `dimension_date` (`id`),
  CONSTRAINT `FK_kpi_view_depth` FOREIGN KEY (`kpi_dimension_id`) REFERENCES `dimension_kpi` (`id`),
  CONSTRAINT `FK_platform_view_depth` FOREIGN KEY (`platform_dimension_id`) REFERENCES `dimension_platform` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='统计用户浏览深度相关分析数据的统计表';

-- ----------------------------
-- Records of stats_view_depth
-- ----------------------------
