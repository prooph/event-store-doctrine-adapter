-- Replace [shortclassname] with the lowercase short classname of your aggregate roots, e.g. My\Model\User = user = user_stream

CREATE TABLE IF NOT EXISTS `[shortclassname]_stream` (
  `event_id` varchar(36) COLLATE utf8_unicode_ci NOT NULL,
  `version` int(11) NOT NULL,
  `event_name` varchar(100) COLLATE utf8_unicode_ci NOT NULL,
  `event_class` varchar(100) COLLATE utf8_unicode_ci NOT NULL,
  `payload` text COLLATE utf8_unicode_ci NOT NULL,
  `created_at` varchar(50) COLLATE utf8_unicode_ci NOT NULL,
  `aggregate_id` text COLLATE utf8_unicode_ci NOT NULL,
  PRIMARY KEY (`event_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;