CREATE TABLE `reliable_msg_queues` (
  `id` varchar(255) NOT NULL default '',
  `queue` varchar(255) NOT NULL default '',
  `headers` text NOT NULL,
  `object` blob NOT NULL,
  PRIMARY KEY  (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=binary;

CREATE TABLE `reliable_msg_topics` (
  `topic` varchar(255) NOT NULL default '',
  `headers` text NOT NULL,
  `object` blob NOT NULL,
  PRIMARY KEY  (`topic`)
) ENGINE=InnoDB DEFAULT CHARSET=binary;
