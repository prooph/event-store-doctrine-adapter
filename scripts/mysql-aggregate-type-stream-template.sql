-- Replace [shortclassname] with the lowercase short classname of your aggregate roots, e.g. My\Model\User = user = user_stream

CREATE TABLE IF NOT EXISTS `[shortclassname]_stream` (
  `eventId` varchar(200) COLLATE utf8_unicode_ci NOT NULL,
  `version` int(11) NOT NULL,
  `eventName` text COLLATE utf8_unicode_ci NOT NULL,
  `payload` text COLLATE utf8_unicode_ci NOT NULL,
  `occurredOn` text COLLATE utf8_unicode_ci NOT NULL,
  `aggregate_id` text COLLATE utf8_unicode_ci NOT NULL,
  PRIMARY KEY (`eventId`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;